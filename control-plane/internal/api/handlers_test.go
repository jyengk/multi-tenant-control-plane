package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"controlplane/internal/domain"
)

// fakeService lets each test script the service's response.
type fakeService struct {
	create  func(domain.CreateTenantInput) (domain.Tenant, domain.Task, error)
	list    func(domain.TenantFilter) (domain.Page[domain.Tenant], error)
	get     func(uuid.UUID) (domain.Tenant, error)
	update  func(uuid.UUID, domain.UpdateTenantInput) (domain.Tenant, domain.Task, error)
	del     func(uuid.UUID) (domain.Tenant, domain.Task, error)
	tasks   func(domain.TaskFilter) (domain.Page[domain.Task], error)
	getTask func(uuid.UUID) (domain.Task, error)
}

func (f *fakeService) CreateTenant(_ context.Context, in domain.CreateTenantInput) (domain.Tenant, domain.Task, error) {
	return f.create(in)
}

func (f *fakeService) ListTenants(_ context.Context, fl domain.TenantFilter) (domain.Page[domain.Tenant], error) {
	return f.list(fl)
}
func (f *fakeService) GetTenant(_ context.Context, id uuid.UUID) (domain.Tenant, error) {
	return f.get(id)
}

func (f *fakeService) UpdateTenant(_ context.Context, id uuid.UUID, in domain.UpdateTenantInput) (domain.Tenant, domain.Task, error) {
	return f.update(id, in)
}

func (f *fakeService) DeleteTenant(_ context.Context, id uuid.UUID) (domain.Tenant, domain.Task, error) {
	return f.del(id)
}

func (f *fakeService) ListTasks(_ context.Context, fl domain.TaskFilter) (domain.Page[domain.Task], error) {
	return f.tasks(fl)
}
func (f *fakeService) GetTask(_ context.Context, id uuid.UUID) (domain.Task, error) {
	return f.getTask(id)
}

var fixedTime = time.Date(2026, 10, 12, 5, 30, 0, 123_000_000, time.UTC)

func sampleTenant(status domain.TenantStatus, version int64) domain.Tenant {
	return domain.Tenant{
		ID: uuid.MustParse("0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44"), Slug: "acme-eu", Name: "Acme",
		Status: status, Version: version, CreatedAt: fixedTime, UpdatedAt: fixedTime,
	}
}

func sampleTask(tt domain.TaskType, tenantID uuid.UUID) domain.Task {
	return domain.Task{
		ID: uuid.MustParse("0192f3a1-7c2f-7000-8000-000000000001"), Type: tt, TenantID: tenantID,
		Status: domain.TaskAccepted, CreatedAt: fixedTime, UpdatedAt: fixedTime,
	}
}

func do(t *testing.T, svc Service, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	NewHandler(svc, nil).ServeHTTP(rec, req)
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) errorDetail {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %q", rec.Body.String())
	}
	return body.Error
}

func assertError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) errorDetail {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status: got %d, want %d (body %s)", rec.Code, status, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type: got %q", ct)
	}
	d := errorCode(t, rec)
	if d.Code != code {
		t.Fatalf("code: got %q, want %q", d.Code, code)
	}
	return d
}

// ---- Create ----------------------------------------------------------------

func TestCreateTenant_Accepted(t *testing.T) {
	tenant := sampleTenant(domain.TenantProvisioning, 1)
	svc := &fakeService{create: func(in domain.CreateTenantInput) (domain.Tenant, domain.Task, error) {
		if in.Slug != "acme-eu" || in.Name != "Acme" {
			t.Errorf("unexpected input %+v", in)
		}
		return tenant, sampleTask(domain.TaskDeploy, tenant.ID), nil
	}}

	rec := do(t, svc, "POST", "/v1/tenants", `{"slug":"acme-eu","name":"Acme"}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/v1/tenants/"+tenant.ID.String() {
		t.Errorf("Location: %q", loc)
	}
	var resp mutationResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Tenant.Status != "provisioning" || resp.Task.Type != "deploy" || resp.Task.Status != "accepted" {
		t.Errorf("unexpected body %+v", resp)
	}
	if resp.Tenant.CreatedAt != "2026-10-12T05:30:00.123Z" {
		t.Errorf("timestamp format: %q", resp.Tenant.CreatedAt)
	}
}

func TestCreateTenant_ValidationErrors(t *testing.T) {
	rec := do(t, &fakeService{}, "POST", "/v1/tenants", `{"slug":"Bad_Slug","name":"  "}`)
	d := assertError(t, rec, http.StatusUnprocessableEntity, "validation_error")
	if d.Details["slug"] == "" || d.Details["name"] == "" {
		t.Errorf("expected per-field details, got %+v", d.Details)
	}
}

func TestCreateTenant_DuplicateSlug(t *testing.T) {
	svc := &fakeService{create: func(domain.CreateTenantInput) (domain.Tenant, domain.Task, error) {
		return domain.Tenant{}, domain.Task{}, domain.ErrTenantAlreadyExists
	}}
	rec := do(t, svc, "POST", "/v1/tenants", `{"slug":"acme-eu","name":"Acme"}`)
	assertError(t, rec, http.StatusConflict, "tenant_already_exists")
}

func TestCreateTenant_BadBodies(t *testing.T) {
	for name, body := range map[string]string{
		"malformed":     `{"slug":`,
		"unknown field": `{"slug":"acme-eu","name":"Acme","plan":"gold"}`,
		"wrong type":    `{"slug":123,"name":"Acme"}`,
		"two objects":   `{"slug":"acme-eu","name":"Acme"}{}`,
	} {
		t.Run(name, func(t *testing.T) {
			rec := do(t, &fakeService{}, "POST", "/v1/tenants", body)
			assertError(t, rec, http.StatusBadRequest, "invalid_request")
		})
	}
}

// ---- Read ------------------------------------------------------------------

func TestGetTenant_NotFoundAndMalformedID(t *testing.T) {
	svc := &fakeService{get: func(uuid.UUID) (domain.Tenant, error) {
		return domain.Tenant{}, domain.ErrTenantNotFound
	}}
	assertError(t, do(t, svc, "GET", "/v1/tenants/"+uuid.NewString(), ""), http.StatusNotFound, "tenant_not_found")
	assertError(t, do(t, svc, "GET", "/v1/tenants/not-a-uuid", ""), http.StatusNotFound, "tenant_not_found")
}

func TestListTenants_PagingAndFilter(t *testing.T) {
	next := uuid.New()
	svc := &fakeService{list: func(f domain.TenantFilter) (domain.Page[domain.Tenant], error) {
		if f.Limit != 2 || f.Status == nil || *f.Status != domain.TenantActive {
			t.Errorf("unexpected filter %+v", f)
		}
		return domain.Page[domain.Tenant]{
			Items:      []domain.Tenant{sampleTenant(domain.TenantActive, 2)},
			NextCursor: &next,
		}, nil
	}}
	rec := do(t, svc, "GET", "/v1/tenants?status=active&limit=2", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
	}
	var resp listResponse[tenantResponse]
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Data) != 1 || resp.NextCursor == nil || *resp.NextCursor != next.String() {
		t.Errorf("unexpected page %+v", resp)
	}
}

func TestListTenants_EmptyPageIsArrayNotNull(t *testing.T) {
	svc := &fakeService{list: func(domain.TenantFilter) (domain.Page[domain.Tenant], error) {
		return domain.Page[domain.Tenant]{}, nil
	}}
	rec := do(t, svc, "GET", "/v1/tenants", "")
	if !strings.Contains(rec.Body.String(), `"data":[]`) {
		t.Errorf("empty list must serialise as [], got %s", rec.Body.String())
	}
}

func TestListTenants_InvalidQuery(t *testing.T) {
	rec := do(t, &fakeService{}, "GET", "/v1/tenants?status=deleted&limit=500&cursor=xyz", "")
	d := assertError(t, rec, http.StatusUnprocessableEntity, "validation_error")
	for _, f := range []string{"status", "limit", "cursor"} {
		if d.Details[f] == "" {
			t.Errorf("missing detail for %s: %+v", f, d.Details)
		}
	}
}

// ---- Update ----------------------------------------------------------------

func TestUpdateTenant_Accepted(t *testing.T) {
	tenant := sampleTenant(domain.TenantUpdating, 3)
	svc := &fakeService{update: func(id uuid.UUID, in domain.UpdateTenantInput) (domain.Tenant, domain.Task, error) {
		if in.Version != 2 || in.Name == nil || *in.Name != "Acme EU" {
			t.Errorf("unexpected input %+v", in)
		}
		return tenant, sampleTask(domain.TaskUpdate, id), nil
	}}
	rec := do(t, svc, "PATCH", "/v1/tenants/"+tenant.ID.String(), `{"version":2,"name":"Acme EU"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUpdateTenant_Validation(t *testing.T) {
	id := uuid.NewString()
	cases := map[string]struct{ body, field string }{
		"missing version":   {`{"name":"x"}`, "version"},
		"zero version":      {`{"version":0,"name":"x"}`, "version"},
		"slug is immutable": {`{"version":1,"name":"x","slug":"other"}`, "slug"},
		"no mutable field":  {`{"version":1}`, "name"},
		"empty name":        {`{"version":1,"name":""}`, "name"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			rec := do(t, &fakeService{}, "PATCH", "/v1/tenants/"+id, c.body)
			d := assertError(t, rec, http.StatusUnprocessableEntity, "validation_error")
			if d.Details[c.field] == "" {
				t.Errorf("expected detail for %q, got %+v", c.field, d.Details)
			}
		})
	}
}

func TestUpdateTenant_DomainErrors(t *testing.T) {
	cases := map[string]struct {
		err    error
		status int
		code   string
	}{
		"not found":   {domain.ErrTenantNotFound, 404, "tenant_not_found"},
		"not allowed": {domain.ErrTenantUpdateNotAllowed, 409, "tenant_update_not_allowed"},
		"conflict":    {domain.ErrTenantVersionConflict, 409, "tenant_version_conflict"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			svc := &fakeService{update: func(uuid.UUID, domain.UpdateTenantInput) (domain.Tenant, domain.Task, error) {
				return domain.Tenant{}, domain.Task{}, c.err
			}}
			rec := do(t, svc, "PATCH", "/v1/tenants/"+uuid.NewString(), `{"version":1,"name":"x"}`)
			assertError(t, rec, c.status, c.code)
		})
	}
}

// ---- Delete ----------------------------------------------------------------

func TestDeleteTenant(t *testing.T) {
	tenant := sampleTenant(domain.TenantDestroying, 4)
	ok := &fakeService{del: func(id uuid.UUID) (domain.Tenant, domain.Task, error) {
		return tenant, sampleTask(domain.TaskDestroy, id), nil
	}}
	if rec := do(t, ok, "DELETE", "/v1/tenants/"+tenant.ID.String(), ""); rec.Code != http.StatusAccepted {
		t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
	}

	notAllowed := &fakeService{del: func(uuid.UUID) (domain.Tenant, domain.Task, error) {
		return domain.Tenant{}, domain.Task{}, domain.ErrTenantUpdateNotAllowed
	}}
	assertError(t, do(t, notAllowed, "DELETE", "/v1/tenants/"+tenant.ID.String(), ""),
		http.StatusConflict, "tenant_update_not_allowed")
}

// ---- Tasks -----------------------------------------------------------------

func TestTasks(t *testing.T) {
	tenantID := uuid.New()
	svc := &fakeService{
		tasks: func(f domain.TaskFilter) (domain.Page[domain.Task], error) {
			if f.TenantID == nil || *f.TenantID != tenantID || f.Type == nil || *f.Type != domain.TaskDeploy {
				t.Errorf("unexpected filter %+v", f)
			}
			return domain.Page[domain.Task]{Items: []domain.Task{sampleTask(domain.TaskDeploy, tenantID)}}, nil
		},
		getTask: func(uuid.UUID) (domain.Task, error) { return domain.Task{}, domain.ErrTaskNotFound },
	}
	if rec := do(t, svc, "GET", "/v1/tasks?tenant_id="+tenantID.String()+"&type=deploy", ""); rec.Code != 200 {
		t.Fatalf("list: got %d: %s", rec.Code, rec.Body.String())
	}
	assertError(t, do(t, svc, "GET", "/v1/tasks/"+uuid.NewString(), ""), 404, "task_not_found")
	assertError(t, do(t, svc, "GET", "/v1/tasks/garbage", ""), 404, "task_not_found")
	assertError(t, do(t, svc, "GET", "/v1/tasks?status=queued", ""), 422, "validation_error")
}

// ---- Routing and robustness -----------------------------------------------

func TestRouting_StructuredMethodNotAllowedAndNotFound(t *testing.T) {
	rec := do(t, &fakeService{}, "PUT", "/v1/tenants/"+uuid.NewString(), `{}`)
	assertError(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
	if allow := rec.Header().Get("Allow"); allow != "GET, PATCH, DELETE" {
		t.Errorf("Allow: %q", allow)
	}
	// Tasks are read-only over HTTP.
	assertError(t, do(t, &fakeService{}, "POST", "/v1/tasks", `{}`), 405, "method_not_allowed")
	assertError(t, do(t, &fakeService{}, "GET", "/v2/whatever", ""), 404, "route_not_found")
}

func TestUnexpectedErrorsDoNotLeak(t *testing.T) {
	svc := &fakeService{get: func(uuid.UUID) (domain.Tenant, error) {
		return domain.Tenant{}, errors.New("pq: password authentication failed for user admin")
	}}
	rec := do(t, svc, "GET", "/v1/tenants/"+uuid.NewString(), "")
	d := assertError(t, rec, http.StatusInternalServerError, "internal_error")
	if strings.Contains(d.Message, "pq") {
		t.Errorf("internal details leaked: %q", d.Message)
	}
}

func TestPanicBecomesStructured500(t *testing.T) {
	svc := &fakeService{get: func(uuid.UUID) (domain.Tenant, error) { panic("boom") }}
	rec := do(t, svc, "GET", "/v1/tenants/"+uuid.NewString(), "")
	assertError(t, rec, http.StatusInternalServerError, "internal_error")
}

func TestHealth(t *testing.T) {
	if rec := do(t, &fakeService{}, "GET", "/healthz", ""); rec.Code != 200 {
		t.Errorf("healthz: %d", rec.Code)
	}
	req := httptest.NewRequest("GET", "/readyz", nil)
	rec := httptest.NewRecorder()
	NewHandler(&fakeService{}, func(context.Context) error { return errors.New("db down") }).ServeHTTP(rec, req)
	assertError(t, rec, http.StatusServiceUnavailable, "not_ready")
}