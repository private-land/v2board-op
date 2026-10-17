// Package rotation implements the opt-in durable IP rotation worker.
package rotation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var ErrLeaseLost = errors.New("rotation lease lost")

type Target struct {
	ID   int    `json:"id"`
	Type string `json:"type"`
	Name string `json:"name"`
	Host string `json:"host"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
}
type Task struct {
	ID                string   `json:"id"`
	Route             string   `json:"route"`
	SourceIP          string   `json:"source_ip"`
	TargetIP          string   `json:"target_ip"`
	InstanceName      string   `json:"instance_name"`
	Stage             string   `json:"stage"`
	LeaseToken        string   `json:"lease_token"`
	Targets           []Target `json:"targets"`
	RegistrationReady bool     `json:"registration_ready"`
}
type Config struct {
	Enabled          bool     `json:"enabled"`
	StreamKeys       []string `json:"stream_keys"`
	HeartbeatSeconds int      `json:"heartbeat_seconds"`
	ReconcileSeconds int      `json:"reconcile_seconds"`
}
type Host struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}
type API interface {
	Config(context.Context, []string) (Config, error)
	Claim(context.Context, []string, string) (*Task, error)
	Heartbeat(context.Context, *Task) error
	Progress(context.Context, *Task, string, string) (*Task, error)
	Finish(context.Context, *Task, string, string) error
	Probe(context.Context, []Host) (map[string]*bool, error)
	ChangeIP(context.Context, *Task) error
}
type Client struct {
	BaseURL, Token string
	HTTP           *http.Client
}

func (c *Client) request(ctx context.Context, method, path string, query url.Values, body, out any) error {
	return c.requestInternal(ctx, method, "/rotation"+path, query, body, out)
}

func (c *Client) requestInternal(ctx context.Context, method, path string, query url.Values, body, out any) error {
	if query == nil {
		query = url.Values{}
	}
	query.Set("token", c.Token)
	var data bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&data).Encode(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+"/api/v1/server/internal"+path+"?"+query.Encode(), &data)
	if err != nil {
		return errors.New("invalid rotation API URL")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	h := c.HTTP
	if h == nil {
		h = &http.Client{Timeout: 15 * time.Second}
	}
	res, err := h.Do(req)
	if err != nil {
		return errors.New("rotation API transport failure")
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode >= 300 {
		return responseError(res)
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
		return err
	}
	return json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(out)
}

// APIError exposes only a bounded, recognized business code, never response
// bodies or token-bearing request URLs.
type APIError struct {
	StatusCode int
	Code       string
}

func (e *APIError) Error() string {
	if e.Code == "stale_rotation_lease" {
		return ErrLeaseLost.Error()
	}
	if e.Code != "" {
		return fmt.Sprintf("rotation API status %d (%s)", e.StatusCode, e.Code)
	}
	return fmt.Sprintf("rotation API status %d", e.StatusCode)
}

func (e *APIError) Is(target error) bool {
	return target == ErrLeaseLost && e.StatusCode == http.StatusConflict && e.Code == "stale_rotation_lease"
}

func responseError(res *http.Response) error {
	code := res.Header.Get("X-Rotation-Error")
	switch code {
	case "stale_rotation_lease", "rotation_generation_mismatch", "rotation_target_identity_missing", "rotation_configuration_changed", "registration_generation_conflict", "registration_identity_ambiguous":
	default:
		code = ""
	}
	if res.StatusCode == http.StatusConflict && code == "" {
		// Older panels expose these exact messages without the machine-readable header.
		var body struct {
			Message string `json:"message"`
		}
		if json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&body) == nil {
			switch body.Message {
			case "Stale lease", "Stale rotation lease":
				code = "stale_rotation_lease"
			case "Instance registration no longer matches source or target IP":
				code = "registration_generation_conflict"
			case "Rotation generation mismatch":
				code = "rotation_generation_mismatch"
			}
		}
	}
	return &APIError{StatusCode: res.StatusCode, Code: code}
}

// ChangeIP commits the AWS-confirmed IP to the panel before DNS reconciliation.
// Retries use the saved IP; the panel makes registration updates and notices idempotent.
func (c *Client) ChangeIP(ctx context.Context, task *Task) error {
	seen := map[string]bool{}
	for _, target := range task.Targets {
		key := fmt.Sprintf("%s:%d", target.Type, target.ID)
		if seen[key] {
			continue
		}
		seen[key] = true
		body := map[string]any{"type": target.Type, "id": target.ID, "source_ip": task.SourceIP, "target_ip": task.TargetIP}
		body["rotation_task_id"] = task.ID
		body["lease_token"] = task.LeaseToken
		if task.InstanceName != "" {
			body["instance_name"] = task.InstanceName
		}
		var result struct {
			Data bool `json:"data"`
		}
		if err := c.requestInternal(ctx, "POST", "/changeIp", nil, body, &result); err != nil {
			return err
		}
		if !result.Data {
			return errors.New("panel rejected IP update")
		}
	}
	return nil
}
func (c *Client) Config(ctx context.Context, tags []string) (Config, error) {
	var r struct {
		Data Config `json:"data"`
	}
	q := url.Values{"tags[]": tags}
	err := c.request(ctx, "GET", "/config", q, nil, &r)
	return r.Data, err
}
func (c *Client) Claim(ctx context.Context, tags []string, worker string) (*Task, error) {
	var r struct {
		Data *Task `json:"data"`
	}
	err := c.request(ctx, "POST", "/claim", nil, map[string]any{"tags": tags, "worker_id": worker}, &r)
	return r.Data, err
}
func (c *Client) Heartbeat(ctx context.Context, t *Task) error {
	return c.request(ctx, "POST", "/"+url.PathEscape(t.ID)+"/heartbeat", nil, map[string]string{"lease_token": t.LeaseToken}, nil)
}
func (c *Client) Progress(ctx context.Context, t *Task, stage, ip string) (*Task, error) {
	var r struct {
		Data *Task `json:"data"`
	}
	b := map[string]string{"lease_token": t.LeaseToken, "stage": stage}
	if ip != "" {
		b["target_ip"] = ip
	}
	if t.InstanceName != "" {
		b["instance_name"] = t.InstanceName
	}
	err := c.request(ctx, "POST", "/"+url.PathEscape(t.ID)+"/progress", nil, b, &r)
	if err == nil && r.Data == nil {
		err = errors.New("missing task response")
	}
	return r.Data, err
}
func (c *Client) Finish(ctx context.Context, t *Task, outcome, reason string) error {
	if len(reason) > 1000 {
		reason = reason[:1000]
	}
	return c.request(ctx, "POST", "/"+url.PathEscape(t.ID)+"/finish", nil, map[string]string{"lease_token": t.LeaseToken, "outcome": outcome, "error": reason}, nil)
}
func (c *Client) Probe(ctx context.Context, hosts []Host) (map[string]*bool, error) {
	var r struct {
		Data map[string]*bool `json:"data"`
	}
	err := c.request(ctx, "POST", "/probe", nil, map[string]any{"hosts": hosts}, &r)
	return r.Data, err
}
