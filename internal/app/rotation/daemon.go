package rotation

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errNoWake = errors.New("no wakeup")

type Wakeup interface {
	Snapshot(context.Context, []string) error
	Wait(context.Context) error
}
type Daemon struct {
	API         API
	Wake        Wakeup
	Tags        []string
	WorkerID    string
	Concurrency int
	NewCloud    func() (Cloud, error)
}

func (d *Daemon) Run(ctx context.Context) error {
	if len(d.Tags) == 0 || d.WorkerID == "" || d.Concurrency < 1 || d.NewCloud == nil || d.API == nil || d.Wake == nil {
		return errors.New("invalid rotation daemon configuration")
	}
	conf, err := d.API.Config(ctx, d.Tags)
	if err != nil {
		return err
	}
	if !conf.Enabled {
		return nil
	}
	if conf.ReconcileSeconds <= 0 {
		conf.ReconcileSeconds = 300
	}
	if conf.HeartbeatSeconds <= 0 {
		conf.HeartbeatSeconds = 30
	}
	events := make(chan struct{}, 1)
	signal := func() {
		select {
		case events <- struct{}{}:
		default:
		}
	}
	// Snapshot precedes the first authoritative reconciliation. Redis failures
	// never prevent API reconciliation; retry is bounded and reconnect wakes it.
	_ = d.Wake.Snapshot(ctx, conf.StreamKeys)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for runCtx.Err() == nil {
			if err := d.Wake.Wait(runCtx); err != nil {
				if errors.Is(err, errNoWake) {
					continue
				}
				if !pause(runCtx, time.Second) {
					return
				}
				if d.Wake.Snapshot(runCtx, conf.StreamKeys) == nil {
					signal()
				}
				continue
			}
			signal()
		}
	}()
	defer readers.Wait()
	ticker := time.NewTicker(time.Duration(conf.ReconcileSeconds) * time.Second)
	defer ticker.Stop()
	completed := make(chan string, d.Concurrency)
	active := map[string]bool{}
	var workers sync.WaitGroup
	defer workers.Wait()
	signal()
	for {
		select {
		case <-ctx.Done():
			cancel()
			return nil
		case id := <-completed:
			delete(active, id)
			signal()
		case <-ticker.C:
			signal()
		case <-events:
			for len(active) < d.Concurrency && ctx.Err() == nil {
				task, err := d.API.Claim(ctx, d.Tags, d.WorkerID)
				if err != nil {
					break
				}
				if task == nil {
					break
				}
				if active[task.ID] {
					break
				}
				active[task.ID] = true
				workers.Add(1)
				go func(task *Task) {
					defer workers.Done()
					defer func() { completed <- task.ID }()
					cloud, err := d.NewCloud()
					if err != nil {
						_ = d.API.Finish(runCtx, task, "retry", "cloud initialization failed")
						return
					}
					(&Executor{API: d.API, Cloud: cloud}).Execute(runCtx, task, time.Duration(conf.HeartbeatSeconds)*time.Second)
				}(task)
			}
		}
	}
}
func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
