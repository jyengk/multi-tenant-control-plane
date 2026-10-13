package store

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"controlplane/internal/domain"
)

// Integration tests: need TEST_DATABASE_URL (see testPool in db_test.go),
// otherwise they skip.

func newTestStore(t *testing.T) *Store {
	t.Helper()
	pool := testPool(t)
	if err := ApplySchema(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return New(pool)
}

// setStatus simulates the worker finishing a task (the consumer doesn't exist
// yet): closes open tasks and moves the tenant.
func setStatus(t *testing.T, s *Store, id uuid.UUID, status domain.TenantStatus) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `UPDATE tasks SET status = 'done' WHERE tenant_id = $1 AND status IN ('accepted','in_progress')`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE tenants SET status = $2, version = version + 1 WHERE id = $1`, id, string(status)); err != nil {
		t.Fatal(err)
	}
}

func TestCreateTenant_WritesTenantAndOutboxTask(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	tenant, task, err := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: "acme-eu", Name: "Acme"})
	if err != nil {
		t.Fatal(err)
	}
	if tenant.Status != domain.TenantProvisioning || tenant.Version != 1 {
		t.Errorf("tenant: %+v", tenant)
	}
	if task.Type != domain.TaskDeploy || task.Status != domain.TaskAccepted || task.TenantID != tenant.ID {
		t.Errorf("task: %+v", task)
	}

	var unpublished bool
	if err := s.pool.QueryRow(ctx, `SELECT published_at IS NULL FROM tasks WHERE id = $1`, task.ID).Scan(&unpublished); err != nil || !unpublished {
		t.Errorf("task should be in the outbox (published_at NULL): %v %v", unpublished, err)
	}

	got, err := s.GetTenant(ctx, tenant.ID)
	if err != nil || got.Slug != "acme-eu" {
		t.Errorf("get: %+v %v", got, err)
	}
}

func TestCreateTenant_ConcurrentSameSlug_ExactlyOneWins(t *testing.T) {
	s := newTestStore(t)
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := s.CreateTenant(context.Background(), domain.CreateTenantInput{Slug: "race", Name: "x"})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, domain.ErrTenantAlreadyExists):
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("want exactly 1 winner, got %d", wins)
	}
}

func TestUpdateTenant_GuardsAndErrors(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenant, _, _ := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: "acme", Name: "Acme"})
	name := "Acme EU"

	// Still provisioning → not allowed.
	if _, _, err := s.UpdateTenant(ctx, tenant.ID, domain.UpdateTenantInput{Version: 1, Name: &name}); !errors.Is(err, domain.ErrTenantUpdateNotAllowed) {
		t.Errorf("provisioning: %v", err)
	}

	setStatus(t, s, tenant.ID, domain.TenantActive) // version → 2

	// Stale version → conflict.
	if _, _, err := s.UpdateTenant(ctx, tenant.ID, domain.UpdateTenantInput{Version: 1, Name: &name}); !errors.Is(err, domain.ErrTenantVersionConflict) {
		t.Errorf("stale version: %v", err)
	}
	// Unknown id → not found.
	if _, _, err := s.UpdateTenant(ctx, uuid.New(), domain.UpdateTenantInput{Version: 1, Name: &name}); !errors.Is(err, domain.ErrTenantNotFound) {
		t.Errorf("unknown: %v", err)
	}
	// Correct version → updating, version 3, new name, update task.
	got, task, err := s.UpdateTenant(ctx, tenant.ID, domain.UpdateTenantInput{Version: 2, Name: &name})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.TenantUpdating || got.Version != 3 || got.Name != name || task.Type != domain.TaskUpdate {
		t.Errorf("after update: %+v %+v", got, task)
	}
}

// The brief's concurrency requirement: two simultaneous PATCHes with the same
// version must not both succeed.
func TestUpdateTenant_ConcurrentSameVersion_ExactlyOneWins(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenant, _, _ := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: "acme", Name: "Acme"})
	setStatus(t, s, tenant.ID, domain.TenantActive) // version 2

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := "renamed"
			_, _, err := s.UpdateTenant(context.Background(), tenant.ID, domain.UpdateTenantInput{Version: 2, Name: &name})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, domain.ErrTenantUpdateNotAllowed), errors.Is(err, domain.ErrTenantVersionConflict):
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("want exactly 1 winner, got %d", wins)
	}
}

func TestDeleteTenant(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenant, _, _ := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: "acme", Name: "Acme"})

	if _, _, err := s.DeleteTenant(ctx, tenant.ID); !errors.Is(err, domain.ErrTenantUpdateNotAllowed) {
		t.Errorf("delete while provisioning: %v", err)
	}
	setStatus(t, s, tenant.ID, domain.TenantFailed) // delete is allowed from failed
	got, task, err := s.DeleteTenant(ctx, tenant.ID)
	if err != nil || got.Status != domain.TenantDestroying || task.Type != domain.TaskDestroy {
		t.Fatalf("delete: %+v %+v %v", got, task, err)
	}
	// Destroyed tenants keep their slug reserved.
	setStatus(t, s, tenant.ID, domain.TenantDestroyed)
	if _, _, err := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: "acme", Name: "Again"}); !errors.Is(err, domain.ErrTenantAlreadyExists) {
		t.Errorf("reuse destroyed slug: %v", err)
	}
}

func TestListTenantsAndTasks_Pagination(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	var ids []uuid.UUID
	for _, slug := range []string{"aaa", "bbb", "ccc"} {
		tn, _, err := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: slug, Name: slug})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, tn.ID)
	}

	p1, err := s.ListTenants(ctx, domain.TenantFilter{Limit: 2})
	if err != nil || len(p1.Items) != 2 || p1.NextCursor == nil {
		t.Fatalf("page 1: %+v %v", p1, err)
	}
	p2, err := s.ListTenants(ctx, domain.TenantFilter{Limit: 2, Cursor: p1.NextCursor})
	if err != nil || len(p2.Items) != 1 || p2.NextCursor != nil || p2.Items[0].ID != ids[2] {
		t.Fatalf("page 2: %+v %v", p2, err)
	}

	st := domain.TenantActive
	empty, err := s.ListTenants(ctx, domain.TenantFilter{Limit: 10, Status: &st})
	if err != nil || len(empty.Items) != 0 {
		t.Fatalf("status filter: %+v %v", empty, err)
	}

	tasks, err := s.ListTasks(ctx, domain.TaskFilter{Limit: 10, TenantID: &ids[0]})
	if err != nil || len(tasks.Items) != 1 || tasks.Items[0].Type != domain.TaskDeploy {
		t.Fatalf("tasks for tenant: %+v %v", tasks, err)
	}
	if _, err := s.GetTask(ctx, uuid.New()); !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("unknown task: %v", err)
	}
}