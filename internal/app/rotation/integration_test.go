package rotation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// This test uses a disposable Laravel router/database and a private local Redis.
// Its Cloud never constructs an AWS SDK or changes a real IP/DNS record.
func TestLaravelRedisIntegration(t *testing.T) {
	base := os.Getenv("ROTATION_INTEGRATION_API")
	if base == "" {
		t.Skip("set ROTATION_INTEGRATION_API to the local tests/Fixtures/rotation-api.php server")
	}
	client := &Client{BaseURL: base, Token: "rotation-fixture-token", HTTP: &http.Client{Timeout: 10 * time.Second}}
	observed := &fixtureAPI{Client: client, claimed: map[string]time.Time{}, finished: map[string]string{}}
	state := &fixtureCloudState{base: base, rotations: map[string]int{}, dns: map[string]int{}, ips: map[string]string{}}
	redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:16389", DB: 15})
	defer func() { _ = redisClient.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	wake := &fixtureWake{RedisWake: &RedisWake{Client: redisClient}}
	daemon := &Daemon{API: observed, Wake: wake, Tags: []string{"lightsail_cf-fixture"}, WorkerID: "integration-worker", Concurrency: 3, NewCloud: func() (Cloud, error) {
		state.mu.Lock()
		state.initialized++
		state.mu.Unlock()
		return &fixtureCloud{state}, nil
	}}
	go func() { stopped <- daemon.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-stopped:
			if err != nil {
				t.Errorf("daemon: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon failed to stop")
		}
	}()
	// Drain any seed task from fixture smoke checks, then establish a quiet baseline.
	time.Sleep(600 * time.Millisecond)
	state.mu.Lock()
	before := state.initialized
	state.mu.Unlock()
	observed.mu.Lock()
	claimsBefore := observed.claims
	observed.mu.Unlock()
	time.Sleep(time.Second)
	state.mu.Lock()
	idleInitializations := state.initialized - before
	state.mu.Unlock()
	observed.mu.Lock()
	idleClaims := observed.claims - claimsBefore
	observed.mu.Unlock()
	if idleInitializations != 0 || idleClaims != 0 {
		t.Fatalf("idle work: cloud_init=%d claims=%d", idleInitializations, idleClaims)
	}

	const samples = 100
	latencies := make([]float64, 0, samples)
	for i := 0; i < samples; i++ {
		started := time.Now()
		var created struct {
			ID       string  `json:"id"`
			QueuedAt float64 `json:"queued_at"`
		}
		fixturePost(t, base, "enqueue", map[string]any{}, &created)
		waitFixture(t, 10*time.Second, func() bool {
			observed.mu.Lock()
			defer observed.mu.Unlock()
			return observed.finished[created.ID] == "completed"
		})
		observed.mu.Lock()
		claimed := observed.claimed[created.ID]
		observed.mu.Unlock()
		if claimed.IsZero() {
			t.Fatal("completed without observed claim")
		}
		latencies = append(latencies, float64(claimed.Sub(started).Microseconds())/1000)
		state.mu.Lock()
		rotations := state.rotations[created.ID]
		state.mu.Unlock()
		if rotations != 1 {
			t.Fatalf("task %s rotated %d times", created.ID, rotations)
		}
	}
	sort.Float64s(latencies)
	p50, p95, p99 := latencies[49], latencies[94], latencies[98]
	t.Logf("DELIVERY samples=%d P50=%.2fms P95=%.2fms P99=%.2fms idle_cloud_calls=%d idle_claims=%d", samples, p50, p95, p99, idleInitializations, idleClaims)
	if p95 > 3000 || p99 > 10000 {
		t.Fatalf("delivery objective missed P95=%.2f P99=%.2f", p95, p99)
	}

	// Simulate the agent unregistering during stop/start. A rebuilt registration
	// must not strand DNS on the old IP; a registration gap must not block it.
	for _, mode := range []string{"rebuild", "gap"} {
		state.mu.Lock()
		state.registrationMode = mode
		state.mu.Unlock()
		var created struct {
			ID string `json:"id"`
		}
		fixturePost(t, base, "enqueue", map[string]any{}, &created)
		waitFixture(t, 10*time.Second, func() bool {
			observed.mu.Lock()
			defer observed.mu.Unlock()
			return observed.finished[created.ID] == "completed"
		})
		var result struct {
			Task              Task  `json:"task"`
			Logs              int   `json:"logs"`
			RegistrationIDs   []int `json:"registration_ids"`
			RegistrationReady bool  `json:"registration_ready"`
		}
		fixturePost(t, base, "task", map[string]any{"task_id": created.ID}, &result)
		state.mu.Lock()
		rotations, dns := state.rotations[created.ID], state.dns[created.ID]
		state.mu.Unlock()
		if rotations != 1 || dns != 1 || result.Logs != 1 || result.Task.Stage != "dns_updated" {
			t.Fatalf("%s follow-up: rotations=%d dns=%d result=%+v", mode, rotations, dns, result)
		}
		if mode == "rebuild" {
			if len(result.RegistrationIDs) != 1 || !result.RegistrationReady {
				t.Fatalf("rebuilt registration not recognized: %+v", result)
			}
		} else if len(result.RegistrationIDs) != 0 || result.RegistrationReady {
			t.Fatalf("registration gap falsely reported ready: %+v", result)
		}
		t.Logf("%s: panel receipt, DNS update and completion; exactly one cloud rotation", mode)
	}

	// Fail DNS after obtaining an IP. The durable checkpoint must survive a retry.
	state.mu.Lock()
	state.failNextDNS = true
	state.mu.Unlock()
	var created struct {
		ID string `json:"id"`
	}
	fixturePost(t, base, "enqueue", map[string]any{}, &created)
	waitFixture(t, 10*time.Second, func() bool {
		observed.mu.Lock()
		defer observed.mu.Unlock()
		return observed.finished[created.ID] == "retry"
	})
	var pending struct {
		Task            Task     `json:"task"`
		Logs            int      `json:"logs"`
		RegistrationIPs []string `json:"registration_ips"`
	}
	fixturePost(t, base, "task", map[string]any{"task_id": created.ID}, &pending)
	if pending.Task.Stage != "ip_acquired" || pending.Task.TargetIP == "" {
		t.Fatalf("missing durable checkpoint: %+v", pending.Task)
	}
	if pending.Logs != 1 || len(pending.RegistrationIPs) != 1 || pending.RegistrationIPs[0] != pending.Task.TargetIP {
		t.Fatalf("changeIp did not commit before DNS/agent registration: %+v", pending)
	}
	// Stop the entire consumer, then resume from the persisted checkpoint.
	cancel()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go func() { stopped <- daemon.Run(ctx) }()
	fixturePost(t, base, "expedite", map[string]any{"task_id": created.ID}, nil)
	waitFixture(t, 10*time.Second, func() bool {
		observed.mu.Lock()
		defer observed.mu.Unlock()
		return observed.finished[created.ID] == "completed"
	})
	state.mu.Lock()
	rotations, dnsCalls := state.rotations[created.ID], state.dns[created.ID]
	state.mu.Unlock()
	if rotations != 1 || dnsCalls != 2 {
		t.Fatalf("DNS retry repeated mutation: rotations=%d dns=%d", rotations, dnsCalls)
	}
	var completed struct {
		Task struct {
			Status string `json:"status"`
		} `json:"task"`
		Logs int `json:"logs"`
	}
	fixturePost(t, base, "task", map[string]any{"task_id": created.ID}, &completed)
	if completed.Task.Status != "completed" || completed.Logs != 1 {
		t.Fatalf("completion/logs: %+v", completed)
	}
	t.Log("DNS recovery after worker restart: durable ip_acquired resumed, one cloud rotation, one completion log")

	// Interrupt the Redis transport while SQL/API remain available. Reconnection
	// must reconcile the durable task even when its wakeup is duplicated.
	wake.failed.Store(true)
	waitFixture(t, 5*time.Second, func() bool { return wake.failures.Load() > 0 })
	var outageTask struct {
		ID string `json:"id"`
	}
	fixturePost(t, base, "enqueue", map[string]any{}, &outageTask)
	conf, err := client.Config(ctx, []string{"lightsail_cf-fixture"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := redisClient.XAdd(ctx, &redis.XAddArgs{Stream: conf.StreamKeys[0], Values: map[string]any{"task_id": outageTask.ID}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	wake.failed.Store(false)
	waitFixture(t, 10*time.Second, func() bool {
		observed.mu.Lock()
		defer observed.mu.Unlock()
		return observed.finished[outageTask.ID] == "completed"
	})
	state.mu.Lock()
	rotations = state.rotations[outageTask.ID]
	state.mu.Unlock()
	if rotations != 1 {
		t.Fatalf("Redis reconnect/duplicates rotated %d times", rotations)
	}
	t.Log("Redis transport interruption and 20 duplicate wakeups: recovered, one rotation")
}

type fixtureAPI struct {
	*Client
	mu       sync.Mutex
	claimed  map[string]time.Time
	finished map[string]string
	claims   int
}

func (f *fixtureAPI) Claim(ctx context.Context, tags []string, worker string) (*Task, error) {
	v, err := f.Client.Claim(ctx, tags, worker)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	if v != nil {
		f.claimed[v.ID] = time.Now()
	}
	return v, err
}
func (f *fixtureAPI) Finish(ctx context.Context, task *Task, outcome, reason string) error {
	err := f.Client.Finish(ctx, task, outcome, reason)
	if err == nil {
		f.mu.Lock()
		f.finished[task.ID] = outcome
		f.mu.Unlock()
	}
	return err
}

type fixtureCloudState struct {
	mu               sync.Mutex
	base             string
	rotations, dns   map[string]int
	ips              map[string]string
	initialized      int
	failNextDNS      bool
	registrationMode string
}
type fixtureCloud struct{ s *fixtureCloudState }

func (f *fixtureCloud) Inspect(ctx context.Context, task *Task) (string, error) {
	task.InstanceName = "fixture-" + task.ID
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	if ip := f.s.ips[task.ID]; ip != "" {
		return ip, nil
	}
	return task.SourceIP, nil
}
func (f *fixtureCloud) Rotate(ctx context.Context, task *Task) (string, error) {
	f.s.mu.Lock()
	f.s.rotations[task.ID]++
	ip := "198.51.100.42"
	f.s.ips[task.ID] = ip
	mode := f.s.registrationMode
	f.s.registrationMode = ""
	f.s.mu.Unlock()
	if mode != "" {
		if err := fixtureRequest(ctx, f.s.base, "reregister", map[string]any{"task_id": task.ID, "ip": ip, "mode": mode}, nil); err != nil {
			return "", err
		}
	}
	return ip, nil
}
func (f *fixtureCloud) DNS(ctx context.Context, task *Task) error {
	f.s.mu.Lock()
	f.s.dns[task.ID]++
	fail := f.s.failNextDNS
	if fail {
		f.s.failNextDNS = false
	}
	f.s.mu.Unlock()
	if fail {
		return fmt.Errorf("injected DNS failure")
	}
	return fixtureRequest(ctx, f.s.base, "register", map[string]any{"task_id": task.ID}, nil)
}
func fixturePost(t *testing.T, base, path string, input, out any) {
	t.Helper()
	if err := fixtureRequest(context.Background(), base, path, input, out); err != nil {
		t.Fatal(err)
	}
}
func fixtureRequest(ctx context.Context, base, path string, input, out any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", base+"/__fixture/"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != 200 {
		return fmt.Errorf("fixture %s status %d", path, res.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}
	return nil
}
func waitFixture(t *testing.T, limit time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("fixture operation timed out")
}

// Inject a Redis transport failure without disrupting unrelated local services.
type fixtureWake struct {
	*RedisWake
	failed   atomic.Bool
	failures atomic.Int32
}

func (w *fixtureWake) Snapshot(ctx context.Context, keys []string) error {
	if w.failed.Load() {
		w.failures.Add(1)
		return fmt.Errorf("injected Redis transport failure")
	}
	return w.RedisWake.Snapshot(ctx, keys)
}
func (w *fixtureWake) Wait(ctx context.Context) error {
	if w.failed.Load() {
		w.failures.Add(1)
		return fmt.Errorf("injected Redis transport failure")
	}
	return w.RedisWake.Wait(ctx)
}
