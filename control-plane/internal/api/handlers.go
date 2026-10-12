package api

import (
	"net/http"
)

// handlers holds the dependencies every handler needs. svc is nil until the
// store is wired in main.go.
type handlers struct {
	svc Service
}

// ---- Tenants ---------------------------------------------------------------

// createTenant handles POST /v1/tenants.
func (h *handlers) createTenant(w http.ResponseWriter, r *http.Request) {
	// TODO:
	// 1. Decode body {slug, name} strictly (unknown fields → 400 invalid_request).
	// 2. Validate: domain.ValidateSlug, domain.ValidateName → 422 validation_error.
	// 3. h.svc.CreateTenant(...) → tenant (provisioning) + deploy task (accepted).
	// 4. 202 Accepted, Location: /v1/tenants/{id}, body {tenant, task}.
	notImplemented(w, r, "createTenant")
}

// listTenants handles GET /v1/tenants?status=&limit=&cursor=.
func (h *handlers) listTenants(w http.ResponseWriter, r *http.Request) {
	// TODO:
	// 1. Parse status (must be a valid TenantStatus), limit (1-100, default 20), cursor (UUID).
	// 2. h.svc.ListTenants(...).
	// 3. 200 OK, body {data: [...], next_cursor}.
	notImplemented(w, r, "listTenants")
}

// getTenant handles GET /v1/tenants/{id}.
func (h *handlers) getTenant(w http.ResponseWriter, r *http.Request) {
	// TODO:
	// 1. Parse r.PathValue("id") as UUID; malformed → 404 tenant_not_found.
	// 2. h.svc.GetTenant(...).
	// 3. 200 OK, body = tenant.
	notImplemented(w, r, "getTenant")
}

// updateTenant handles PATCH /v1/tenants/{id}.
func (h *handlers) updateTenant(w http.ResponseWriter, r *http.Request) {
	// TODO:
	// 1. Parse id; decode body {version, name}.
	// 2. Validate: version required (>= 1), name non-empty, slug rejected (immutable).
	// 3. h.svc.UpdateTenant(...) → errors: not_found 404, update_not_allowed 409, version_conflict 409.
	// 4. 202 Accepted, body {tenant (updating), task (update)}.
	notImplemented(w, r, "updateTenant")
}

// deleteTenant handles DELETE /v1/tenants/{id}.
func (h *handlers) deleteTenant(w http.ResponseWriter, r *http.Request) {
	// TODO:
	// 1. Parse id.
	// 2. h.svc.DeleteTenant(...) → allowed from active or failed, else 409 tenant_update_not_allowed.
	// 3. 202 Accepted, body {tenant (destroying), task (destroy)}.
	notImplemented(w, r, "deleteTenant")
}

// ---- Tasks -----------------------------------------------------------------

// listTasks handles GET /v1/tasks?tenant_id=&status=&type=&limit=&cursor=.
func (h *handlers) listTasks(w http.ResponseWriter, r *http.Request) {
	// TODO:
	// 1. Parse filters: tenant_id (UUID), status, type, limit, cursor.
	// 2. h.svc.ListTasks(...).
	// 3. 200 OK, body {data: [...], next_cursor}.
	notImplemented(w, r, "listTasks")
}

// getTask handles GET /v1/tasks/{id}.
func (h *handlers) getTask(w http.ResponseWriter, r *http.Request) {
	// TODO:
	// 1. Parse id; malformed → 404 task_not_found.
	// 2. h.svc.GetTask(...).
	// 3. 200 OK, body = task.
	notImplemented(w, r, "getTask")
}

// ---- Helpers ---------------------------------------------------------------

// notImplemented is a placeholder response; delete each call as the handler
// gets implemented.
func notImplemented(w http.ResponseWriter, r *http.Request, handler string) {
	writeErrorBody(w, http.StatusNotImplemented, "not_implemented",
		handler+" is not implemented yet",
		map[string]string{"method": r.Method, "path": r.URL.Path, "id": r.PathValue("id")})
}
