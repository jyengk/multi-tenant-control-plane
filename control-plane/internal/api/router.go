package api

import (
	"context"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"
)

// ReadyFunc reports whether the service can serve traffic (e.g. DB ping).
type ReadyFunc func(ctx context.Context) error

// NewHandler builds the HTTP handler: the route table plus middleware.
//
// Routing uses the standard library ServeMux (Go 1.22+ method and wildcard
// patterns). Each path also gets a method-less fallback so a wrong method
// returns a structured 405 instead of the mux's plain-text one.
func NewHandler(svc Service, ready ReadyFunc) http.Handler {
	h := &handlers{svc: svc}
	mux := http.NewServeMux()

	// Tenants: full CRUD.
	route(mux, "/v1/tenants", map[string]http.HandlerFunc{
		http.MethodPost: h.createTenant,
		http.MethodGet:  h.listTenants,
	})
	route(mux, "/v1/tenants/{id}", map[string]http.HandlerFunc{
		http.MethodGet:    h.getTenant,
		http.MethodPatch:  h.updateTenant,
		http.MethodDelete: h.deleteTenant,
	})

	// Tasks: read-only.
	route(mux, "/v1/tasks", map[string]http.HandlerFunc{
		http.MethodGet: h.listTasks,
	})
	route(mux, "/v1/tasks/{id}", map[string]http.HandlerFunc{
		http.MethodGet: h.getTask,
	})

	// Health.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready != nil {
			if err := ready(r.Context()); err != nil {
				writeErrorBody(w, http.StatusServiceUnavailable, "not_ready", err.Error(), nil)
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	// Anything else: structured 404.
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeErrorBody(w, http.StatusNotFound, codeRouteNotFound, "no such route", nil)
	})

	return recoverer(logRequests(mux))
}

// route registers one handler per method on path, plus a fallback that
// answers any other method with 405 and an Allow header.
func route(mux *http.ServeMux, path string, methods map[string]http.HandlerFunc) {
	allow := make([]string, 0, len(methods))
	for method, fn := range methods {
		mux.HandleFunc(method+" "+path, fn)
		allow = append(allow, method)
	}
	slices.SortFunc(allow, func(a, b string) int { return methodOrder[a] - methodOrder[b] })
	allowHeader := strings.Join(allow, ", ")
	mux.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Allow", allowHeader)
		writeErrorBody(w, http.StatusMethodNotAllowed, codeMethodNotAllowed, "method not allowed", nil)
	})
}

// methodOrder keeps the Allow header deterministic.
var methodOrder = map[string]int{
	http.MethodGet: 0, http.MethodPost: 1, http.MethodPatch: 2, http.MethodDelete: 3,
}

// ---- Middleware ------------------------------------------------------------

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		slog.InfoContext(r.Context(), "http request",
			"method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration_ms", time.Since(start).Milliseconds())
	})
}

// recoverer turns a panic into a structured 500 instead of a dropped
// connection or a stack trace in the response.
func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				slog.ErrorContext(r.Context(), "panic in handler", "panic", v, "path", r.URL.Path)
				writeErrorBody(w, http.StatusInternalServerError, codeInternal, "internal server error", nil)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
