package rotation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

var ErrStaleSource = errors.New("source instance no longer current")

// Inspect must freshly verify source IP and fill the actual AWS InstanceName.
// A resumed task must inspect that stored instance, never guess from node names.
type Cloud interface {
	Inspect(context.Context, *Task) (string, error)
	Rotate(context.Context, *Task) (string, error)
	DNS(context.Context, *Task) error
}
type Executor struct {
	API         API
	Cloud       Cloud
	TaskTimeout time.Duration
}

func (e *Executor) Execute(parent context.Context, task *Task, heartbeat time.Duration) {
	if closer, ok := e.Cloud.(interface{ Close() error }); ok {
		defer func() { _ = closer.Close() }()
	}
	timeout := e.TaskTimeout
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	done := make(chan struct{})
	leaseLost := make(chan struct{})
	lease := &Task{ID: task.ID, LeaseToken: task.LeaseToken}
	if e.API.Heartbeat(ctx, lease) != nil {
		return
	}
	fence := func(ctx context.Context) error {
		err := e.API.Heartbeat(ctx, lease)
		if err != nil {
			cancel()
		}
		return err
	}
	if cloud, ok := e.Cloud.(interface {
		SetFence(func(context.Context) error)
	}); ok {
		cloud.SetFence(fence)
	}
	if heartbeat <= 0 {
		heartbeat = 30 * time.Second
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if e.API.Heartbeat(ctx, lease) != nil {
					close(leaseLost)
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-done }()
	finish := func(outcome, reason string) {
		select {
		case <-leaseLost:
			return
		default:
		}
		if ctx.Err() == nil {
			_ = e.API.Finish(ctx, task, outcome, reason)
		}
	}
	retry := func(err error) { finish("retry", err.Error()) }
	progress := func(stage, ip string) error {
		v, err := e.API.Progress(ctx, task, stage, ip)
		if err == nil {
			task = v
		}
		return err
	}
	if len(task.Targets) == 0 {
		finish("cancelled", "no DNS references")
		return
	}
	for _, v := range task.Targets {
		if v.Host == "" || net.ParseIP(v.Host) != nil || v.Port < 1 {
			finish("cancelled", "unsupported reference")
			return
		}
	}
	if task.TargetIP == "" {
		if task.Stage != "rotating" {
			if err := progress("prechecking", ""); err != nil {
				return
			}
			hosts := hostsFor(task, task.SourceIP)
			results, err := e.API.Probe(ctx, hosts)
			if err != nil {
				retry(err)
				return
			}
			failed := false
			for _, h := range hosts {
				v := results[fmt.Sprintf("%s:%d", h.Host, h.Port)]
				if v == nil {
					retry(errors.New("source probe unknown"))
					return
				}
				if !*v {
					failed = true
				}
			}
			if !failed {
				finish("cancelled", "source recovered")
				return
			}
		}
		current, err := e.Cloud.Inspect(ctx, task)
		if err != nil {
			if errors.Is(err, ErrStaleSource) {
				finish("cancelled", err.Error())
				return
			}
			retry(err)
			return
		}
		if task.Stage == "rotating" {
			if current == task.SourceIP || net.ParseIP(current) == nil {
				retry(errors.New("ambiguous rotation requires manual inspection"))
				return
			}
		} else {
			if current != task.SourceIP {
				finish("cancelled", "source instance IP changed")
				return
			}
			if err := progress("rotating", ""); err != nil {
				return
			}
			if ctx.Err() != nil {
				return
			}
			if err := fence(ctx); err != nil {
				return
			}
			current, err = e.Cloud.Rotate(ctx, task)
			if err != nil {
				retry(err)
				return
			}
		}
		if net.ParseIP(current) == nil || current == task.SourceIP {
			retry(errors.New("rotation produced no new IP"))
			return
		}
		if err := progress("ip_acquired", current); err != nil {
			return
		}
	}
	if ctx.Err() != nil {
		return
	}
	current, err := e.Cloud.Inspect(ctx, task)
	if err != nil {
		retry(err)
		return
	}
	if current != task.TargetIP {
		finish("superseded", "IP changed; later superseded by another IP; no further mutations")
		return
	}
	if err := fence(ctx); err != nil {
		return
	}
	if err := e.API.ChangeIP(ctx, task); err != nil {
		retry(err)
		return
	}
	if err := e.Cloud.DNS(ctx, task); err != nil {
		retry(err)
		return
	}
	if task.Stage != "verified" {
		if err := progress("dns_updated", task.TargetIP); err != nil {
			return
		}
	}
	// Completion confirms the IP change and DNS reconciliation.
	// Target reachability is monitored separately from this task.
	finish("completed", "")
}
func hostsFor(t *Task, ip string) []Host {
	out := []Host{}
	seen := map[int]bool{}
	for _, v := range t.Targets {
		if !seen[v.Port] {
			seen[v.Port] = true
			out = append(out, Host{ip, v.Port})
		}
	}
	return out
}
