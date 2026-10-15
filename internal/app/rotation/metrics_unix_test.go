//go:build linux || darwin

package rotation

import (
	"context"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestLaravelRedisIdleBudget(t *testing.T) {
	base := os.Getenv("ROTATION_INTEGRATION_API")
	if base == "" {
		t.Skip("requires disposable local Laravel/Redis fixture")
	}
	client := &Client{BaseURL: base, Token: "rotation-fixture-token"}
	api := &fixtureAPI{Client: client, claimed: map[string]time.Time{}, finished: map[string]string{}}
	redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:16389", DB: 15})
	defer func() { _ = redisClient.Close() }()
	var initialized atomic.Int64
	daemon := &Daemon{API: api, Wake: &RedisWake{Client: redisClient}, Tags: []string{"lightsail_cf-fixture"}, WorkerID: "idle-budget", Concurrency: 3, NewCloud: func() (Cloud, error) {
		initialized.Add(1)
		return &fixtureCloud{&fixtureCloudState{base: base, rotations: map[string]int{}, dns: map[string]int{}, ips: map[string]string{}}}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- daemon.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-stopped:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("idle daemon did not stop")
		}
	}()
	time.Sleep(500 * time.Millisecond)
	api.mu.Lock()
	claimsBefore := api.claims
	api.mu.Unlock()
	var before, after syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	time.Sleep(20 * time.Second)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
		t.Fatal(err)
	}
	cpuSeconds := float64(after.Utime.Sec-before.Utime.Sec+after.Stime.Sec-before.Stime.Sec) + float64(after.Utime.Usec-before.Utime.Usec+after.Stime.Usec-before.Stime.Usec)/1e6
	cpuPercent := cpuSeconds / time.Since(start).Seconds() * 100
	rss := float64(after.Maxrss)
	if runtime.GOOS == "linux" {
		rss *= 1024
	}
	rssMiB := rss / (1024 * 1024)
	api.mu.Lock()
	claims := api.claims - claimsBefore
	api.mu.Unlock()
	t.Logf("IDLE seconds=%.1f CPU=%.4f%% peak_RSS=%.2fMiB cloud_initializations=%d API_claims=%d", time.Since(start).Seconds(), cpuPercent, rssMiB, initialized.Load(), claims)
	if cpuPercent > 1 || rssMiB > 128 || initialized.Load() != 0 || claims != 0 {
		t.Fatal("idle resource budget exceeded")
	}
}
