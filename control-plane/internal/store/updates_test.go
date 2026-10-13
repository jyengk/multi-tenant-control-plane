package store

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"controlplane/internal/domain"
)

// Integration tests: need TEST_DATABASE_URL, otherwise they skip.

func apply(t *testing.T, s *Store, taskID uuid.UUID, st domain.TaskStatus, updateID uuid.UUID) UpdateOutcome {
	t.Helper()
	out, err := s.ApplyTaskUpdate(context.Background(), TaskUpdate{UpdateID: updateID, TaskID: taskID, Status: st})
	if err != nil {
		t.Fatalf("apply %s: %v", st, err)
	}
	return out
}

func TestApplyTaskUpdate_HappyPathMovesTenant(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenant, task, _ := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: "acme", Name: "Acme"})

	if out := apply(t, s, task.ID, domain.TaskInProgress, uuid.New()); out != OutcomeApplied {
		t.Fatalf("in_progress: %s", out)
	}
	if got, _ := s.GetTenant(ctx, tenant.ID); got.Status != domain.TenantProvisioning || got.Version != 1 {
		t.Fatalf("in_progress must not move the tenant: %+v", got)
	}
	if out := apply(t, s, task.ID, domain.TaskDone, uuid.New()); out != OutcomeApplied {
		t.Fatalf("done: %s", out)
	}
	got, _ := s.GetTenant(ctx, tenant.ID)
	if got.Status != domain.TenantActive || got.Version != 2 {
		t.Fatalf("after done: want active v2, got %s v%d", got.Status, got.Version)
	}
	if tk, _ := s.GetTask(ctx, task.ID); tk.Status != domain.TaskDone {
		t.Fatalf("task: %s", tk.Status)
	}
}

func TestApplyTaskUpdate_FailedMovesTenantToFailed(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenant, task, _ := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: "acme", Name: "Acme"})
	apply(t, s, task.ID, domain.TaskFailed, uuid.New()) // accepted → failed: forward jump
	if got, _ := s.GetTenant(ctx, tenant.ID); got.Status != domain.TenantFailed {
		t.Fatalf("want failed, got %s", got.Status)
	}
}

func TestApplyTaskUpdate_DuplicateChangesNothing(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenant, task, _ := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: "acme", Name: "Acme"})
	id := uuid.New()

	apply(t, s, task.ID, domain.TaskDone, id)
	if out := apply(t, s, task.ID, domain.TaskDone, id); out != OutcomeDuplicate {
		t.Fatalf("second delivery: %s", out)
	}
	if got, _ := s.GetTenant(ctx, tenant.ID); got.Version != 2 {
		t.Fatalf("duplicate must not bump version again: v%d", got.Version)
	}
}

func TestApplyTaskUpdate_StaleDoesNotRegress(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenant, task, _ := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: "acme", Name: "Acme"})

	apply(t, s, task.ID, domain.TaskDone, uuid.New())
	if out := apply(t, s, task.ID, domain.TaskInProgress, uuid.New()); out != OutcomeStaleIgnored {
		t.Fatalf("in_progress after done: %s", out)
	}
	if out := apply(t, s, task.ID, domain.TaskFailed, uuid.New()); out != OutcomeStaleIgnored {
		t.Fatalf("failed after done: %s", out)
	}
	if tk, _ := s.GetTask(ctx, task.ID); tk.Status != domain.TaskDone {
		t.Fatalf("task regressed to %s", tk.Status)
	}
	if got, _ := s.GetTenant(ctx, tenant.ID); got.Status != domain.TenantActive || got.Version != 2 {
		t.Fatalf("tenant changed: %s v%d", got.Status, got.Version)
	}
}

func TestApplyTaskUpdate_UnknownTask(t *testing.T) {
	s := newTestStore(t)
	_, err := s.ApplyTaskUpdate(context.Background(), TaskUpdate{UpdateID: uuid.New(), TaskID: uuid.New(), Status: domain.TaskDone})
	if !errors.Is(err, ErrUnknownTask) {
		t.Fatalf("want ErrUnknownTask, got %v", err)
	}
}

// The same update delivered to several consumers at once is applied once.
func TestApplyTaskUpdate_ConcurrentDuplicates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenant, task, _ := s.CreateTenant(ctx, domain.CreateTenantInput{Slug: "acme", Name: "Acme"})
	id := uuid.New()

	const n = 10
	var wg sync.WaitGroup
	outs := make(chan UpdateOutcome, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := s.ApplyTaskUpdate(ctx, TaskUpdate{UpdateID: id, TaskID: task.ID, Status: domain.TaskDone})
			if err != nil {
				t.Error(err)
			}
			outs <- out
		}()
	}
	wg.Wait()
	close(outs)
	applied := 0
	for o := range outs {
		if o == OutcomeApplied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("want exactly 1 applied, got %d", applied)
	}
	if got, _ := s.GetTenant(ctx, tenant.ID); got.Version != 2 {
		t.Fatalf("version bumped %d times", got.Version-1)
	}
}