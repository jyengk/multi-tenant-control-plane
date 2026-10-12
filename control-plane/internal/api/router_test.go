package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRoutesAreRegistered proves every endpoint reaches its handler. While
// handlers are stubs they answer 501; once one is implemented, change its
// expected status here (or replace this with real handler tests).
func TestRoutesAreRegistered(t *testing.T) {
	h := NewHandler(nil, nil)
	cases := []struct {
		method, path string
		want         int
	}{
		{"POST", "/v1/tenants", http.StatusNotImplemented},
		{"GET", "/v1/tenants", http.StatusNotImplemented},
		{"GET", "/v1/tenants/abc", http.StatusNotImplemented},
		{"PATCH", "/v1/tenants/abc", http.StatusNotImplemented},
		{"DELETE", "/v1/tenants/abc", http.StatusNotImplemented},
		{"GET", "/v1/tasks", http.StatusNotImplemented},
		{"GET", "/v1/tasks/abc", http.StatusNotImplemented},

		{"GET", "/healthz", http.StatusOK},
		{"GET", "/readyz", http.StatusOK},

		{"PUT", "/v1/tenants/abc", http.StatusMethodNotAllowed},
		{"POST", "/v1/tasks", http.StatusMethodNotAllowed},
		{"GET", "/v2/nothing", http.StatusNotFound},
	}
	for _, c := range cases {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, nil))
			if rec.Code != c.want {
				t.Fatalf("got %d, want %d: %s", rec.Code, c.want, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type %q", ct)
			}
		})
	}
}
