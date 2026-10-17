package rotation

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakeAPI struct {
	task         *Task
	probes       map[string]*bool
	stages       []string
	outcome      string
	heartbeatErr error
	reportErr    error
	reports      int
}

func (f *fakeAPI) Config(context.Context, []string) (Config, error) {
	return Config{Enabled: true, HeartbeatSeconds: 1, ReconcileSeconds: 300}, nil
}
func (f *fakeAPI) Claim(context.Context, []string, string) (*Task, error) {
	v := f.task
	f.task = nil
	return v, nil
}
func (f *fakeAPI) Heartbeat(context.Context, *Task) error { return f.heartbeatErr }
func (f *fakeAPI) ChangeIP(context.Context, *Task) error  { f.reports++; return f.reportErr }
func (f *fakeAPI) Progress(_ context.Context, t *Task, stage, ip string) (*Task, error) {
	v := *t
	v.Stage = stage
	if ip != "" {
		v.TargetIP = ip
	}
	v.RegistrationReady = true
	f.stages = append(f.stages, stage)
	return &v, nil
}
func (f *fakeAPI) Finish(_ context.Context, t *Task, outcome, reason string) error {
	f.outcome = outcome
	return nil
}
func (f *fakeAPI) Probe(context.Context, []Host) (map[string]*bool, error) { return f.probes, nil }

type fakeCloud struct {
	rotations, dns int
	dnsErr         error
	current        string
	wait           bool
}

func (f *fakeCloud) Inspect(context.Context, *Task) (string, error) { return f.current, nil }
func (f *fakeCloud) Rotate(ctx context.Context, t *Task) (string, error) {
	f.rotations++
	if f.wait {
		<-ctx.Done()
		return "", ctx.Err()
	}
	f.current = "2.2.2.2"
	return "2.2.2.2", nil
}
func (f *fakeCloud) DNS(context.Context, *Task) error { f.dns++; return f.dnsErr }
func task() *Task {
	return &Task{ID: "id", SourceIP: "1.1.1.1", Stage: "pending", Targets: []Target{{Host: "node.example.com", Port: 443, Type: "mieru"}}}
}
func TestUnknownNeverRotates(t *testing.T) {
	a := &fakeAPI{probes: map[string]*bool{}}
	c := &fakeCloud{current: "1.1.1.1"}
	e := Executor{API: a, Cloud: c}
	e.Execute(context.Background(), task(), time.Second)
	if c.rotations != 0 || a.outcome != "retry" {
		t.Fatalf("rotations=%d outcome=%s", c.rotations, a.outcome)
	}
}
func TestSavedIPNeverRotatesAfterDNSFailure(t *testing.T) {
	yes := true
	a := &fakeAPI{probes: map[string]*bool{"2.2.2.2:443": &yes}}
	c := &fakeCloud{dnsErr: errors.New("dns"), current: "2.2.2.2"}
	v := task()
	v.Stage = "ip_acquired"
	v.TargetIP = "2.2.2.2"
	e := Executor{API: a, Cloud: c}
	e.Execute(context.Background(), v, time.Second)
	if c.rotations != 0 || a.outcome != "retry" {
		t.Fatal("unsafe DNS recovery")
	}
	c.dnsErr = nil
	e.Execute(context.Background(), v, time.Second)
	if c.rotations != 0 || a.outcome != "completed" {
		t.Fatal("did not resume checkpoint")
	}
}
func TestAmbiguousRotatingNeverRotates(t *testing.T) {
	a := &fakeAPI{}
	c := &fakeCloud{current: "1.1.1.1"}
	v := task()
	v.Stage = "rotating"
	e := Executor{API: a, Cloud: c}
	e.Execute(context.Background(), v, time.Second)
	if c.rotations != 0 || a.outcome != "retry" {
		t.Fatal("ambiguous rotation repeated")
	}
}

type expiringAPI struct {
	*fakeAPI
	calls atomic.Int64
}

func (a *expiringAPI) Heartbeat(ctx context.Context, t *Task) error {
	if a.calls.Add(1) <= 2 {
		return nil
	}
	return ErrLeaseLost
}

func TestHeartbeatLossCancelsCloudWork(t *testing.T) {
	no := false
	a := &fakeAPI{probes: map[string]*bool{"1.1.1.1:443": &no}, heartbeatErr: errors.New("lease")}
	c := &fakeCloud{current: "1.1.1.1", wait: true}
	e := Executor{API: &expiringAPI{fakeAPI: a}, Cloud: c}
	start := time.Now()
	e.Execute(context.Background(), task(), time.Millisecond)
	if time.Since(start) > time.Second || c.dns != 0 || a.outcome != "" || c.rotations != 1 {
		t.Fatal("lease loss continued work")
	}
}

type fakeWake struct{ snapshots, reads int }

func (w *fakeWake) Snapshot(context.Context, []string) error { w.snapshots++; return nil }
func (w *fakeWake) Wait(ctx context.Context) error           { w.reads++; <-ctx.Done(); return ctx.Err() }
func TestIdleDaemonNeverInitializesCloud(t *testing.T) {
	a := &fakeAPI{}
	w := &fakeWake{}
	calls := 0
	d := Daemon{API: a, Wake: w, Tags: []string{"route"}, WorkerID: "worker", Concurrency: 2, NewCloud: func() (Cloud, error) { calls++; return &fakeCloud{}, nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = d.Run(ctx)
	if calls != 0 || w.snapshots != 1 || w.reads != 1 {
		t.Fatalf("cloud=%d snapshots=%d reads=%d", calls, w.snapshots, w.reads)
	}
}

func TestRotationModePreservesLegacyAndMieruDynamic(t *testing.T) {
	cases := []struct {
		kind   string
		static bool
		count  int
		want   string
	}{{"mieru", false, 0, "restart"}, {"mieru", true, 0, "release_restart"}, {"anytls", false, 0, "allocate"}, {"anytls", false, 5, "restart"}, {"anytls", true, 1, "release"}}
	for _, v := range cases {
		if got := rotationMode(v.kind, v.static, v.count); got != v.want {
			t.Errorf("%+v got %s", v, got)
		}
	}
}

func TestExpiredLeaseCannotTouchSavedIPDNS(t *testing.T) {
	a := &fakeAPI{heartbeatErr: ErrLeaseLost}
	c := &fakeCloud{}
	v := task()
	v.TargetIP = "2.2.2.2"
	v.Stage = "ip_acquired"
	(&Executor{API: a, Cloud: c}).Execute(context.Background(), v, time.Second)
	if c.dns != 0 {
		t.Fatal("expired lease modified DNS before fencing")
	}
}

type staleCloud struct{ fakeCloud }

func (c *staleCloud) Inspect(context.Context, *Task) (string, error) { return "", ErrStaleSource }
func TestStaleSourceCancelledWithoutMutation(t *testing.T) {
	no := false
	a := &fakeAPI{probes: map[string]*bool{"1.1.1.1:443": &no}}
	c := &staleCloud{}
	(&Executor{API: a, Cloud: c}).Execute(context.Background(), task(), time.Second)
	if a.outcome != "cancelled" || c.rotations != 0 {
		t.Fatal("stale task not cancelled")
	}
}

func TestSavedIPExternallyChangedNeverUpdatesDNS(t *testing.T) {
	a := &fakeAPI{}
	c := &fakeCloud{current: "3.3.3.3"}
	v := task()
	v.TargetIP = "2.2.2.2"
	v.Stage = "ip_acquired"
	(&Executor{API: a, Cloud: c}).Execute(context.Background(), v, time.Second)
	if c.dns != 0 || c.rotations != 0 || a.outcome != "superseded" {
		t.Fatalf("dns=%d rotations=%d outcome=%s", c.dns, c.rotations, a.outcome)
	}
}
func TestTaskTimeoutBoundsCloudWait(t *testing.T) {
	no := false
	a := &fakeAPI{probes: map[string]*bool{"1.1.1.1:443": &no}}
	c := &fakeCloud{current: "1.1.1.1", wait: true}
	e := Executor{API: a, Cloud: c, TaskTimeout: 5 * time.Millisecond}
	start := time.Now()
	e.Execute(context.Background(), task(), time.Second)
	if time.Since(start) > time.Second || c.dns != 0 {
		t.Fatal("task timeout did not bound cloud wait")
	}
}

type unregisteredAPI struct{ *fakeAPI }

func (a *unregisteredAPI) Progress(ctx context.Context, task *Task, stage, ip string) (*Task, error) {
	v, err := a.fakeAPI.Progress(ctx, task, stage, ip)
	if v != nil {
		v.RegistrationReady = false
	}
	return v, err
}

func TestAWSRotationCompletesWithoutRegistration(t *testing.T) {
	yes := true
	a := &unregisteredAPI{&fakeAPI{probes: map[string]*bool{"2.2.2.2:443": &yes}}}
	c := &fakeCloud{current: "2.2.2.2"}
	v := task()
	v.TargetIP = "2.2.2.2"
	v.Stage = "ip_acquired"
	(&Executor{API: a, Cloud: c}).Execute(context.Background(), v, time.Second)
	if a.outcome != "completed" || c.rotations != 0 || c.dns != 1 || a.reports != 1 {
		t.Fatalf("outcome=%s rotations=%d dns=%d", a.outcome, c.rotations, c.dns)
	}
}

func TestPanelIPUpdateFailurePreservesSavedIPForRetry(t *testing.T) {
	a := &fakeAPI{reportErr: errors.New("panel update failed")}
	c := &fakeCloud{current: "2.2.2.2"}
	v := task()
	v.TargetIP = "2.2.2.2"
	v.Stage = "ip_acquired"
	(&Executor{API: a, Cloud: c}).Execute(context.Background(), v, time.Second)
	if a.outcome != "retry" || a.reports != 1 || c.rotations != 0 || c.dns != 0 {
		t.Fatalf("outcome=%s reports=%d rotations=%d dns=%d", a.outcome, a.reports, c.rotations, c.dns)
	}
}

type noTargetProbeAPI struct {
	*fakeAPI
	t            *testing.T
	sourceProbes int
}

func (a *noTargetProbeAPI) Probe(_ context.Context, hosts []Host) (map[string]*bool, error) {
	for _, host := range hosts {
		if host.Host != "1.1.1.1" {
			a.t.Fatal("IP-change completion must not probe target reachability")
		}
	}
	a.sourceProbes++
	return a.probes, nil
}

func (a *noTargetProbeAPI) Finish(ctx context.Context, task *Task, outcome, reason string) error {
	if reason != "" {
		a.t.Fatalf("successful IP change returned an error: %s", reason)
	}
	return a.fakeAPI.Finish(ctx, task, outcome, reason)
}

func TestIPChangeCompletesWithoutTargetProbe(t *testing.T) {
	for _, stage := range []string{"ip_acquired", "dns_updated", "verified"} {
		t.Run(stage, func(t *testing.T) {
			a := &noTargetProbeAPI{fakeAPI: &fakeAPI{}, t: t}
			c := &fakeCloud{current: "2.2.2.2"}
			v := task()
			v.TargetIP = "2.2.2.2"
			v.Stage = stage
			(&Executor{API: a, Cloud: c}).Execute(context.Background(), v, time.Second)
			if a.outcome != "completed" || a.reports != 1 || c.dns != 1 || c.rotations != 0 || a.sourceProbes != 0 {
				t.Fatalf("outcome=%s reports=%d dns=%d rotations=%d", a.outcome, a.reports, c.dns, c.rotations)
			}
		})
	}
}

func TestFreshIPChangeOnlyProbesSource(t *testing.T) {
	no := false
	a := &noTargetProbeAPI{fakeAPI: &fakeAPI{probes: map[string]*bool{"1.1.1.1:443": &no}}, t: t}
	c := &fakeCloud{current: "1.1.1.1"}
	(&Executor{API: a, Cloud: c}).Execute(context.Background(), task(), time.Second)
	if a.outcome != "completed" || a.reports != 1 || c.dns != 1 || c.rotations != 1 || a.sourceProbes != 1 {
		t.Fatalf("outcome=%s reports=%d dns=%d rotations=%d source_probes=%d", a.outcome, a.reports, c.dns, c.rotations, a.sourceProbes)
	}
}
