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
}
type Client struct {
	BaseURL, Token string
	HTTP           *http.Client
}

func (c *Client) request(ctx context.Context, method, path string, query url.Values, body, out any) error {
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
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+"/api/v1/server/internal/rotation"+path+"?"+query.Encode(), &data)
	if err != nil {
		return errors.New("invalid rotation API URL")
	}
	req.Header.Set("Content-Type", "application/json")
	h := c.HTTP
	if h == nil {
		h = &http.Client{Timeout: 15 * time.Second}
	}
	res, err := h.Do(req)
	if err != nil {
		return errors.New("rotation API transport failure")
	}
	defer res.Body.Close()
	if res.StatusCode == 409 {
		return ErrLeaseLost
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("rotation API status %d", res.StatusCode)
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
		return err
	}
	return json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(out)
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
