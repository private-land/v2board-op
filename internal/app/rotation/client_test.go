package rotation

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestChangeIPRequiresPanelConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		wantError bool
	}{
		{"confirmed", 200, `{"data":true}`, false},
		{"rejected", 200, `{"data":false}`, true},
		{"missing confirmation", 200, `{}`, true},
		{"invalid response", 200, `invalid`, true},
		{"server failure", 500, `{"data":true}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/api/v1/server/internal/changeIp" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := &Client{BaseURL: server.URL, HTTP: server.Client()}
			err := client.ChangeIP(context.Background(), task())
			if (err != nil) != tc.wantError {
				t.Fatalf("error=%v wantError=%v", err, tc.wantError)
			}
		})
	}
}

func TestConflictErrorsDistinguishLeaseLoss(t *testing.T) {
	for _, tc := range []struct {
		name, code, body, want string
		lease                  bool
	}{
		{"expired lease", "stale_rotation_lease", `{}`, "rotation lease lost", true},
		{"rebuilt registration conflict", "registration_generation_conflict", `{}`, "registration_generation_conflict", false},
		{"ambiguous identity", "registration_identity_ambiguous", `{}`, "registration_identity_ambiguous", false},
		{"configuration changed", "rotation_configuration_changed", `{}`, "rotation_configuration_changed", false},
		{"legacy lease", "", `{"message":"Stale rotation lease"}`, "rotation lease lost", true},
		{"legacy generation", "", `{"message":"Instance registration no longer matches source or target IP"}`, "registration_generation_conflict", false},
		{"unknown conflict", "", `{"message":"secret-token-should-not-be-logged"}`, "rotation API status 409", false},
		{"untrusted code", "secret-token-should-not-be-logged", `{}`, "rotation API status 409", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Rotation-Error", tc.code)
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			c := &Client{BaseURL: server.URL, Token: "secret-token-should-not-be-logged", HTTP: server.Client()}
			err := c.ChangeIP(context.Background(), task())
			if err == nil || errors.Is(err, ErrLeaseLost) != tc.lease || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v lease=%v want=%s lease=%v", err, errors.Is(err, ErrLeaseLost), tc.want, tc.lease)
			}
			if strings.Contains(err.Error(), "secret-token-should-not-be-logged") {
				t.Fatalf("response leaked into error: %v", err)
			}
		})
	}
}
