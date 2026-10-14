//go:build e2e

package e2e

import (
	"net/http"
	"strings"
	"testing"
)

// The error contract: status codes, error codes, and field details. These need
// no worker, so they run in either mode.

func TestAPIErrors_CreateValidation(t *testing.T) {
	cases := map[string]struct {
		body   any
		status int
		code   string
		field  string
	}{
		"uppercase slug":     {map[string]string{"slug": "Acme", "name": "x"}, 422, "validation_error", "slug"},
		"underscore in slug": {map[string]string{"slug": "ac_me", "name": "x"}, 422, "validation_error", "slug"},
		"slug too short":     {map[string]string{"slug": "ab", "name": "x"}, 422, "validation_error", "slug"},
		"slug too long":      {map[string]string{"slug": "a" + strings.Repeat("b", 28), "name": "x"}, 422, "validation_error", "slug"},
		"slug trailing dash": {map[string]string{"slug": "acme-", "name": "x"}, 422, "validation_error", "slug"},
		"slug starts digit":  {map[string]string{"slug": "1acme", "name": "x"}, 422, "validation_error", "slug"},
		"blank name":         {map[string]string{"slug": slug("ok"), "name": "   "}, 422, "validation_error", "name"},
		"missing fields":     {map[string]string{}, 422, "validation_error", "slug"},
		"malformed JSON":     {`{"slug":`, 400, "invalid_request", ""},
		"unknown field":      {map[string]string{"slug": slug("ok"), "name": "x", "plan": "gold"}, 400, "invalid_request", ""},
		"wrong type":         {map[string]any{"slug": 123, "name": "x"}, 400, "invalid_request", ""},
		"two JSON objects":   {`{"slug":"abc","name":"x"}{}`, 400, "invalid_request", ""},
		"empty body":         {"", 400, "invalid_request", ""},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			r := call(t, "POST", "/v1/tenants", c.body)
			expect(t, r, c.status, c.code)
			if c.field != "" {
				var e APIError
				r.Decode(t, &e)
				if e.Error.Details[c.field] == "" {
					t.Errorf("missing details for %q: %s", c.field, r.Body)
				}
			}
		})
	}
}

func TestAPIErrors_DuplicateSlug(t *testing.T) {
	s := slug("dup")
	createTenant(t, s)
	r := call(t, "POST", "/v1/tenants", map[string]string{"slug": s, "name": "again"})
	expect(t, r, http.StatusConflict, "tenant_already_exists")
}

func TestAPIErrors_PatchValidation(t *testing.T) {
	id := createTenant(t, slug("pval")).Tenant.ID
	cases := map[string]struct {
		body   any
		status int
		code   string
		field  string
	}{
		"missing version":   {map[string]any{"name": "x"}, 422, "validation_error", "version"},
		"zero version":      {map[string]any{"version": 0, "name": "x"}, 422, "validation_error", "version"},
		"slug is immutable": {map[string]any{"version": 1, "name": "x", "slug": "other"}, 422, "validation_error", "slug"},
		"no name":           {map[string]any{"version": 1}, 422, "validation_error", "name"},
		"empty name":        {map[string]any{"version": 1, "name": ""}, 422, "validation_error", "name"},
		"unknown field":     {map[string]any{"version": 1, "name": "x", "plan": 1}, 400, "invalid_request", ""},
		"version as string": {map[string]any{"version": "1", "name": "x"}, 400, "invalid_request", ""},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			r := call(t, "PATCH", "/v1/tenants/"+id, c.body)
			expect(t, r, c.status, c.code)
			if c.field != "" {
				var e APIError
				r.Decode(t, &e)
				if e.Error.Details[c.field] == "" {
					t.Errorf("missing details for %q: %s", c.field, r.Body)
				}
			}
		})
	}
}

func TestAPIErrors_NotFound(t *testing.T) {
	const unknown = "00000000-0000-7000-8000-000000000000"
	cases := []struct {
		method, path string
		body         any
		code         string
	}{
		{"GET", "/v1/tenants/" + unknown, nil, "tenant_not_found"},
		{"GET", "/v1/tenants/not-a-uuid", nil, "tenant_not_found"},
		{"PATCH", "/v1/tenants/" + unknown, map[string]any{"version": 1, "name": "x"}, "tenant_not_found"},
		{"DELETE", "/v1/tenants/" + unknown, nil, "tenant_not_found"},
		{"GET", "/v1/tasks/" + unknown, nil, "task_not_found"},
		{"GET", "/v1/tasks/garbage", nil, "task_not_found"},
		{"GET", "/v2/anything", nil, "route_not_found"},
	}
	for _, c := range cases {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			expect(t, call(t, c.method, c.path, c.body), http.StatusNotFound, c.code)
		})
	}
}

func TestAPIErrors_MethodNotAllowed(t *testing.T) {
	r := call(t, "PUT", "/v1/tenants/00000000-0000-7000-8000-000000000000", map[string]any{})
	expect(t, r, http.StatusMethodNotAllowed, "method_not_allowed")
	if r.Header.Get("Allow") == "" {
		t.Error("405 must carry an Allow header")
	}
	// Tasks are read-only over HTTP.
	for _, m := range []string{"POST", "PATCH", "DELETE"} {
		path := "/v1/tasks"
		if m != "POST" {
			path += "/00000000-0000-7000-8000-000000000000"
		}
		expect(t, call(t, m, path, map[string]any{}), http.StatusMethodNotAllowed, "method_not_allowed")
	}
}

func TestAPIErrors_BadListQueries(t *testing.T) {
	for _, q := range []string{
		"/v1/tenants?status=deleted",
		"/v1/tenants?limit=0",
		"/v1/tenants?limit=101",
		"/v1/tenants?limit=abc",
		"/v1/tenants?cursor=not-a-uuid",
		"/v1/tasks?status=queued",
		"/v1/tasks?type=migrate",
		"/v1/tasks?tenant_id=nope",
	} {
		t.Run(q, func(t *testing.T) {
			expect(t, call(t, "GET", q, nil), http.StatusUnprocessableEntity, "validation_error")
		})
	}
}

// Every error response, whatever the cause, is structured JSON.
func TestAPIErrors_AlwaysJSON(t *testing.T) {
	for _, r := range []Response{
		call(t, "GET", "/v2/nope", nil),
		call(t, "PUT", "/v1/tenants", "{}"),
		call(t, "POST", "/v1/tenants", "{"),
	} {
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("status %d: content-type %q", r.Status, ct)
		}
		if r.Code() == "" {
			t.Errorf("status %d: no error code in %s", r.Status, r.Body)
		}
	}
}
