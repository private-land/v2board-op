package rotation

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/lightsail"
	"github.com/cloudflare/cloudflare-go"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMieruSDKUsesOnlyDynamicPrimitives(t *testing.T) {
	calls := []string{}
	newIP := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op := r.Header.Get("X-Amz-Target")
		op = op[strings.LastIndex(op, ".")+1:]
		calls = append(calls, op)
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		switch op {
		case "GetInstance":
			ip := "1.1.1.1"
			if newIP {
				ip = "2.2.2.2"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"instance": map[string]any{"name": "actual-instance", "publicIpAddress": ip, "isStaticIp": false, "state": map[string]string{"name": "running"}}})
		case "GetStaticIps":
			w.Write([]byte(`{"staticIps":[]}`))
		case "GetInstanceState":
			state := "stopped"
			if newIP {
				state = "running"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"state": map[string]string{"name": state}})
		case "StartInstance":
			newIP = true
			w.Write([]byte(`{}`))
		case "StopInstance":
			w.Write([]byte(`{}`))
		default:
			t.Errorf("forbidden SDK operation %s", op)
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	client := lightsail.NewFromConfig(aws.Config{Region: "ap-northeast-2", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}, func(o *lightsail.Options) { o.BaseEndpoint = aws.String(server.URL) })
	c := &SDKCloud{AWS: client}
	v := task()
	v.InstanceName = "actual-instance"
	ip, err := c.Rotate(context.Background(), v)
	if err != nil || ip != "2.2.2.2" {
		t.Fatalf("%s %v calls=%v", ip, err, calls)
	}
}
func TestResumedDNSAcquiresInstanceLockAndPreservesEveryReference(t *testing.T) {
	records := map[string][]map[string]any{"one.example.com": {{"id": "old-one", "type": "A", "name": "one.example.com", "content": "1.1.1.1", "ttl": 60}, {"id": "dup-one", "type": "A", "name": "one.example.com", "content": "1.1.1.1", "ttl": 60}}, "two.example.com": {{"id": "old-two", "type": "A", "name": "two.example.com", "content": "1.1.1.1", "ttl": 60}}}
	deleted := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Target") != "" {
			w.Header().Set("Content-Type", "application/x-amz-json-1.1")
			w.Write([]byte(`{"instance":{"name":"stub-dns-resume","publicIpAddress":"2.2.2.2","isStaticIp":false,"state":{"name":"running"}}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		respond := func(v any) {
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "result": v, "result_info": map[string]int{"page": 1, "total_pages": 1, "count": 1, "total_count": 1}})
		}
		if r.URL.Path == "/zones" {
			respond([]map[string]string{{"id": "zone", "name": "example.com"}})
			return
		}
		switch r.Method {
		case "GET":
			respond(records[r.URL.Query().Get("name")])
		case "PUT", "PATCH":
			var v map[string]any
			_ = json.NewDecoder(r.Body).Decode(&v)
			for host, rr := range records {
				for i, old := range rr {
					if strings.HasSuffix(r.URL.Path, "/"+old["id"].(string)) {
						v["id"] = old["id"]
						records[host][i] = v
						respond(v)
						return
					}
				}
			}
			t.Error("missing update record")
		case "DELETE":
			for host, rr := range records {
				found := false
				for _, old := range rr {
					if old["content"] == "2.2.2.2" {
						found = true
					}
				}
				if !found {
					t.Errorf("deleted old before %s was ready", host)
				}
			}
			deleted++
			respond(map[string]string{"id": "dup-one"})
		default:
			t.Errorf("unexpected %s", r.Method)
		}
	}))
	defer server.Close()
	cf, err := cloudflare.New("key", "email", cloudflare.UsingRateLimit(10000), cloudflare.UsingRetryPolicy(0, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	cf.BaseURL = server.URL
	client := lightsail.NewFromConfig(aws.Config{Region: "ap-northeast-2", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}, func(o *lightsail.Options) { o.BaseEndpoint = aws.String(server.URL) })
	c := &SDKCloud{CF: cf, AWS: client}
	defer c.Close()
	v := task()
	v.InstanceName = "stub-dns-resume"
	v.TargetIP = "2.2.2.2"
	v.Targets = []Target{{Host: "one.example.com"}, {Host: "two.example.com"}}
	if err := c.DNS(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	if c.lock == nil {
		t.Fatal("saved-IP DNS recovery did not acquire instance lock")
	}
	if deleted != 1 {
		t.Fatalf("deletes %d", deleted)
	}
}

func TestSDKFenceStopsMutationAfterLeaseLoss(t *testing.T) {
	mutations := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op := r.Header.Get("X-Amz-Target")
		op = op[strings.LastIndex(op, ".")+1:]
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		switch op {
		case "GetInstance":
			w.Write([]byte(`{"instance":{"name":"fenced-instance","publicIpAddress":"1.1.1.1","isStaticIp":false}}`))
		case "GetStaticIps":
			w.Write([]byte(`{"staticIps":[]}`))
		default:
			mutations++
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	client := lightsail.NewFromConfig(aws.Config{Region: "ap-northeast-2", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}, func(o *lightsail.Options) { o.BaseEndpoint = aws.String(server.URL) })
	c := &SDKCloud{AWS: client}
	c.SetFence(func(context.Context) error { return ErrLeaseLost })
	v := task()
	v.InstanceName = "fenced-instance"
	_, err := c.Rotate(context.Background(), v)
	if err != ErrLeaseLost || mutations != 0 {
		t.Fatalf("err=%v mutations=%d", err, mutations)
	}
}

func TestAWSStateWaitHasDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.Write([]byte(`{"state":{"name":"pending"}}`))
	}))
	defer server.Close()
	client := lightsail.NewFromConfig(aws.Config{Region: "ap-northeast-2", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}, func(o *lightsail.Options) { o.BaseEndpoint = aws.String(server.URL) })
	c := &SDKCloud{AWS: client, StateTimeout: 5 * time.Millisecond}
	start := time.Now()
	if err := c.wait(context.Background(), "instance", "running"); err == nil || time.Since(start) > time.Second {
		t.Fatal("unbounded AWS state wait")
	}
}

func TestMieruRecoveredIPMustBeDynamic(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.Write([]byte(`{"instance":{"name":"mieru-static-recovery","publicIpAddress":"2.2.2.2","isStaticIp":true,"state":{"name":"running"}}}`))
	}))
	defer server.Close()
	client := lightsail.NewFromConfig(aws.Config{Region: "ap-northeast-2", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}, func(o *lightsail.Options) { o.BaseEndpoint = aws.String(server.URL) })
	c := &SDKCloud{AWS: client}
	defer c.Close()
	v := task()
	v.InstanceName = "mieru-static-recovery"
	v.Stage = "rotating"
	if _, err := c.Inspect(context.Background(), v); err == nil {
		t.Fatal("Mieru recovery accepted externally attached static address")
	}
}
