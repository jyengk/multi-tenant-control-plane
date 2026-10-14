package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

const swaggerOrigin = "http://localhost:8081"

// inner records whether the wrapped handler ran.
func inner(called *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*called = true
		w.WriteHeader(http.StatusOK)
	})
}

func TestCORS_AllowedOriginGetsHeaders(t *testing.T) {
	var called bool
	req := httptest.NewRequest("GET", "/v1/tenants", nil)
	req.Header.Set("Origin", swaggerOrigin)
	rec := httptest.NewRecorder()
	CORS(swaggerOrigin, inner(&called)).ServeHTTP(rec, req)

	if !called || rec.Code != http.StatusOK {
		t.Fatalf("request should pass through: called=%v code=%d", called, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != swaggerOrigin {
		t.Errorf("Allow-Origin: %q", got)
	}
}

func TestCORS_PreflightAnsweredWithoutCallingHandler(t *testing.T) {
	var called bool
	req := httptest.NewRequest("OPTIONS", "/v1/tenants/abc", nil)
	req.Header.Set("Origin", swaggerOrigin)
	req.Header.Set("Access-Control-Request-Method", "PATCH")
	rec := httptest.NewRecorder()
	CORS(swaggerOrigin, inner(&called)).ServeHTTP(rec, req)

	if called {
		t.Error("preflight must not reach the API handler")
	}
	if rec.Code != http.StatusNoContent {
		t.Errorf("status: %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET, POST, PATCH, DELETE" {
		t.Errorf("Allow-Methods: %q", got)
	}
}

func TestCORS_OtherOriginGetsNothing(t *testing.T) {
	var called bool
	req := httptest.NewRequest("OPTIONS", "/v1/tenants", nil)
	req.Header.Set("Origin", "http://evil.example")
	req.Header.Set("Access-Control-Request-Method", "DELETE")
	rec := httptest.NewRecorder()
	CORS(swaggerOrigin, inner(&called)).ServeHTTP(rec, req)

	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("a different origin must not be allowed")
	}
	if !called {
		t.Error("non-matching requests pass through unchanged (the router answers them)")
	}
}

func TestCORS_NoOriginHeaderUnchanged(t *testing.T) {
	var called bool
	rec := httptest.NewRecorder()
	CORS(swaggerOrigin, inner(&called)).ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if !called || rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("curl/Postman-style requests must be untouched")
	}
}

func TestCORS_DisabledWhenOriginEmpty(t *testing.T) {
	var called bool
	h := inner(&called)
	req := httptest.NewRequest("GET", "/v1/tenants", nil)
	req.Header.Set("Origin", swaggerOrigin)
	rec := httptest.NewRecorder()
	CORS("", h).ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" || !called {
		t.Error("empty origin must disable CORS entirely")
	}
}