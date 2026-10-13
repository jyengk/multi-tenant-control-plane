package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"controlplane/internal/domain"
)

const maxBodyBytes = 1 << 20 // 1 MiB

type handlers struct {
	svc Service
}

// ---- Tenants ---------------------------------------------------------------

// POST /v1/tenants → 202 {tenant, task}
func (h *handlers) createTenant(w http.ResponseWriter, r *http.Request) {
	var req createTenantRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	fields := map[string]string{}
	if !domain.ValidateSlug(req.Slug) {
		fields["slug"] = "must be 3-28 chars matching ^[a-z][-a-z0-9]{1,26}[a-z0-9]$"
	}
	if !domain.ValidateName(req.Name) {
		fields["name"] = "must not be empty"
	}
	if len(fields) > 0 {
		writeError(w, r, domain.NewValidationError(fields))
		return
	}

	tenant, task, err := h.svc.CreateTenant(r.Context(), domain.CreateTenantInput{Slug: req.Slug, Name: req.Name})
	if err != nil {
		writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/tenants/"+tenant.ID.String())
	writeJSON(w, http.StatusAccepted, mutationResponse{Tenant: toTenantResponse(tenant), Task: toTaskResponse(task)})
}

// GET /v1/tenants?status=&limit=&cursor= → 200 {data, next_cursor}
func (h *handlers) listTenants(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	fields := map[string]string{}
	f := domain.TenantFilter{}
	f.Limit, f.Cursor = parsePaging(q.Get("limit"), q.Get("cursor"), fields)
	if s := q.Get("status"); s != "" {
		st := domain.TenantStatus(s)
		if !st.Valid() {
			fields["status"] = "unknown tenant status"
		}
		f.Status = &st
	}
	if len(fields) > 0 {
		writeError(w, r, domain.NewValidationError(fields))
		return
	}

	page, err := h.svc.ListTenants(r.Context(), f)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toListResponse(page, toTenantResponse))
}

// GET /v1/tenants/{id} → 200 tenant
func (h *handlers) getTenant(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, domain.ErrTenantNotFound, w)
	if !ok {
		return
	}
	tenant, err := h.svc.GetTenant(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTenantResponse(tenant))
}

// PATCH /v1/tenants/{id}  body: {"version": 3, "name": "..."} → 202 {tenant, task}
func (h *handlers) updateTenant(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, domain.ErrTenantNotFound, w)
	if !ok {
		return
	}
	var req updateTenantRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	fields := map[string]string{}
	switch {
	case req.Version == nil:
		fields["version"] = "is required"
	case *req.Version < 1:
		fields["version"] = "must be >= 1"
	}
	if req.Slug != nil {
		fields["slug"] = "is immutable"
	}
	if req.Name == nil {
		fields["name"] = "at least one mutable field (name) is required"
	} else if !domain.ValidateName(*req.Name) {
		fields["name"] = "must not be empty"
	}
	if len(fields) > 0 {
		writeError(w, r, domain.NewValidationError(fields))
		return
	}

	tenant, task, err := h.svc.UpdateTenant(r.Context(), id, domain.UpdateTenantInput{Version: *req.Version, Name: req.Name})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, mutationResponse{Tenant: toTenantResponse(tenant), Task: toTaskResponse(task)})
}

// DELETE /v1/tenants/{id} → 202 {tenant, task}
//
// Asynchronous: the tenant moves to "destroying" and stays readable; it
// reaches "destroyed" when the worker reports the destroy task done.
func (h *handlers) deleteTenant(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, domain.ErrTenantNotFound, w)
	if !ok {
		return
	}
	tenant, task, err := h.svc.DeleteTenant(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, mutationResponse{Tenant: toTenantResponse(tenant), Task: toTaskResponse(task)})
}

// ---- Tasks -----------------------------------------------------------------

// GET /v1/tasks?tenant_id=&status=&type=&limit=&cursor= → 200 {data, next_cursor}
func (h *handlers) listTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	fields := map[string]string{}
	f := domain.TaskFilter{}
	f.Limit, f.Cursor = parsePaging(q.Get("limit"), q.Get("cursor"), fields)
	if s := q.Get("tenant_id"); s != "" {
		id, err := uuid.Parse(s)
		if err != nil {
			fields["tenant_id"] = "must be a UUID"
		}
		f.TenantID = &id
	}
	if s := q.Get("status"); s != "" {
		st := domain.TaskStatus(s)
		if !st.Valid() {
			fields["status"] = "unknown task status"
		}
		f.Status = &st
	}
	if s := q.Get("type"); s != "" {
		tt := domain.TaskType(s)
		if !tt.Valid() {
			fields["type"] = "unknown task type"
		}
		f.Type = &tt
	}
	if len(fields) > 0 {
		writeError(w, r, domain.NewValidationError(fields))
		return
	}

	page, err := h.svc.ListTasks(r.Context(), f)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toListResponse(page, toTaskResponse))
}

// GET /v1/tasks/{id} → 200 task
func (h *handlers) getTask(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, domain.ErrTaskNotFound, w)
	if !ok {
		return
	}
	task, err := h.svc.GetTask(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskResponse(task))
}

// ---- Helpers ---------------------------------------------------------------

// pathID parses the {id} wildcard. A malformed ID cannot name an existing
// resource, so it is reported as that resource's not_found error.
func pathID(r *http.Request, notFound *domain.Error, w http.ResponseWriter) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, notFound)
		return uuid.Nil, false
	}
	return id, true
}

// decodeJSON strictly decodes the body: size-limited, unknown fields
// rejected, exactly one JSON value. On failure it writes a 400 and
// returns false.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeErrorBody(w, http.StatusBadRequest, codeInvalidRequest, describeDecodeError(err), nil)
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeErrorBody(w, http.StatusBadRequest, codeInvalidRequest, "body must contain a single JSON object", nil)
		return false
	}
	return true
}

func describeDecodeError(err error) string {
	var syn *json.SyntaxError
	var typ *json.UnmarshalTypeError
	var tooBig *http.MaxBytesError
	switch {
	case errors.Is(err, io.EOF):
		return "request body is empty"
	case errors.As(err, &syn):
		return fmt.Sprintf("malformed JSON at offset %d", syn.Offset)
	case errors.As(err, &typ):
		return fmt.Sprintf("field %q has the wrong type", typ.Field)
	case errors.As(err, &tooBig):
		return "request body too large"
	default:
		// Covers `json: unknown field "x"` from DisallowUnknownFields.
		return err.Error()
	}
}

// parsePaging validates limit and cursor, recording problems in fields.
func parsePaging(limitStr, cursorStr string, fields map[string]string) (int, *uuid.UUID) {
	limit := domain.DefaultPageLimit
	if limitStr != "" {
		n, err := strconv.Atoi(limitStr)
		if err != nil || n < 1 || n > domain.MaxPageLimit {
			fields["limit"] = fmt.Sprintf("must be an integer between 1 and %d", domain.MaxPageLimit)
		} else {
			limit = n
		}
	}
	var cursor *uuid.UUID
	if cursorStr != "" {
		c, err := uuid.Parse(cursorStr)
		if err != nil {
			fields["cursor"] = "invalid cursor"
		} else {
			cursor = &c
		}
	}
	return limit, cursor
}