package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"controlplane/internal/domain"
)

// Store implements api.Service on Postgres.
//
// Concurrency model: every state change is ONE conditional UPDATE whose WHERE
// clause encodes the guard (status, and version for PATCH). Postgres row
// locking makes that statement atomic, so two concurrent PATCHes with the
// same version cannot both match: the first bumps the version, the second
// matches zero rows. No application-level locks are needed, and it stays
// correct across replicas.
type Store struct {
	pool     *pgxpool.Pool
	onCommit func() // called after a task is committed (wakes the outbox relay)
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// OnTaskCommitted registers fn to run after every committed task insert.
// main wires the outbox relay's Nudge here, so new tasks are published in
// milliseconds instead of waiting for the relay's next poll. It runs AFTER
// commit, never inside the transaction.
func (s *Store) OnTaskCommitted(fn func()) { s.onCommit = fn }

func (s *Store) notify() {
	if s.onCommit != nil {
		s.onCommit()
	}
}

// SQLSTATE codes we branch on.
const sqlUniqueViolation = "23505"

const tenantCols = `id, slug, name, status, version, created_at, updated_at`
const taskCols = `id, type, tenant_id, status, created_at, updated_at`

// ---- Tenants: writes --------------------------------------------------------

// CreateTenant inserts the tenant (provisioning) and its deploy task
// (accepted, unpublished) in one transaction. The outbox relay publishes the
// task after commit; nothing is published here.
func (s *Store) CreateTenant(ctx context.Context, in domain.CreateTenantInput) (domain.Tenant, domain.Task, error) {
	tr := domain.TransitionFor(domain.OpCreate)
	var tenant domain.Tenant
	var task domain.Task

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO tenants (id, slug, name, status, version, created_at, updated_at)
			VALUES ($1, $2, $3, $4, 1, now(), now())
			RETURNING `+tenantCols,
			newID(), in.Slug, in.Name, string(tr.To))
		var err error
		if tenant, err = scanTenant(row); err != nil {
			// The UNIQUE constraint settles concurrent creates: exactly one
			// insert wins, the others get 23505. Destroyed tenants keep
			// their row, so their slugs stay taken.
			if pgCode(err) == sqlUniqueViolation {
				return domain.ErrTenantAlreadyExists
			}
			return fmt.Errorf("insert tenant: %w", err)
		}
		task, err = insertTask(ctx, tx, tr.TaskType, tenant.ID)
		return err
	})
	if err == nil {
		s.notify()
	}
	return tenant, task, err
}

// UpdateTenant applies a PATCH: only from active, only if version matches.
func (s *Store) UpdateTenant(ctx context.Context, id uuid.UUID, in domain.UpdateTenantInput) (domain.Tenant, domain.Task, error) {
	return s.transition(ctx, id, domain.OpUpdate, &in.Version, in.Name)
}

// DeleteTenant starts a destroy: from active or failed. No version check.
func (s *Store) DeleteTenant(ctx context.Context, id uuid.UUID) (domain.Tenant, domain.Task, error) {
	return s.transition(ctx, id, domain.OpDelete, nil, nil)
}

// transition is the shared PATCH/DELETE path: one guarded UPDATE, then the
// new task, in one transaction. If the UPDATE matches nothing, a re-read
// decides which error to return.
func (s *Store) transition(ctx context.Context, id uuid.UUID, op domain.Operation,
	version *int64, name *string,
) (domain.Tenant, domain.Task, error) {
	tr := domain.TransitionFor(op)
	var tenant domain.Tenant
	var task domain.Task

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE tenants
			   SET status     = $2,
			       name       = COALESCE($3, name),
			       version    = version + 1,
			       updated_at = now()
			 WHERE id = $1
			   AND status = ANY($4)
			   AND ($5::bigint IS NULL OR version = $5)
			RETURNING `+tenantCols,
			id, string(tr.To), name, statusStrings(tr.From), version)
		var err error
		tenant, err = scanTenant(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return explainNoMatch(ctx, tx, id, tr.From)
		}
		if err != nil {
			return fmt.Errorf("update tenant: %w", err)
		}
		task, err = insertTask(ctx, tx, tr.TaskType, tenant.ID)
		return err
	})
	if err == nil {
		s.notify()
	}
	return tenant, task, err
}

// explainNoMatch re-reads the tenant to pick the right error after a guarded
// UPDATE matched zero rows. Precedence: not_found → update_not_allowed →
// version_conflict (a busy tenant can't be updated even with a fresh version,
// so "not allowed" is the more useful answer).
func explainNoMatch(ctx context.Context, tx pgx.Tx, id uuid.UUID, allowed []domain.TenantStatus) error {
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM tenants WHERE id = $1`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrTenantNotFound
	}
	if err != nil {
		return fmt.Errorf("re-read tenant: %w", err)
	}
	if !containsStatus(allowed, domain.TenantStatus(status)) {
		return domain.ErrTenantUpdateNotAllowed
	}
	// Status is allowed, so the version didn't match (or the row changed between
	// our UPDATE and this read). Either way: refetch and retry.
	return domain.ErrTenantVersionConflict
}

// insertTask creates an accepted, unpublished task (published_at NULL = in
// the outbox).
func insertTask(ctx context.Context, tx pgx.Tx, tt domain.TaskType, tenantID uuid.UUID) (domain.Task, error) {
	row := tx.QueryRow(ctx, `
		INSERT INTO tasks (id, type, tenant_id, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, now(), now())
		RETURNING `+taskCols,
		newID(), string(tt), tenantID, string(domain.TaskAccepted))
	task, err := scanTask(row)
	if err != nil {
		// tasks_one_open_per_tenant: unreachable while the status guards
		// hold; if it ever fires, refuse rather than create a second task.
		if pgCode(err) == sqlUniqueViolation {
			return domain.Task{}, domain.ErrTenantUpdateNotAllowed
		}
		return domain.Task{}, fmt.Errorf("insert task: %w", err)
	}
	return task, nil
}

// ---- Tenants: reads ---------------------------------------------------------

// GetTenant returns one tenant.
func (s *Store) GetTenant(ctx context.Context, id uuid.UUID) (domain.Tenant, error) {
	t, err := scanTenant(s.pool.QueryRow(ctx, `SELECT `+tenantCols+` FROM tenants WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Tenant{}, domain.ErrTenantNotFound
	}
	return t, err
}

// ListTenants returns a page of tenants in id (creation) order. It fetches
// limit+1 rows: the extra row only signals that another page exists.
func (s *Store) ListTenants(ctx context.Context, f domain.TenantFilter) (domain.Page[domain.Tenant], error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+tenantCols+` FROM tenants
		 WHERE ($1::text IS NULL OR status = $1)
		   AND ($2::uuid IS NULL OR id > $2)
		 ORDER BY id
		 LIMIT $3`,
		optString(f.Status), f.Cursor, f.Limit+1)
	if err != nil {
		return domain.Page[domain.Tenant]{}, fmt.Errorf("list tenants: %w", err)
	}
	items, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Tenant, error) { return scanTenant(r) })
	if err != nil {
		return domain.Page[domain.Tenant]{}, fmt.Errorf("list tenants: %w", err)
	}
	return page(items, f.Limit, func(t domain.Tenant) uuid.UUID { return t.ID }), nil
}

// ---- Tasks: reads -----------------------------------------------------------

// GetTask returns one task.
func (s *Store) GetTask(ctx context.Context, id uuid.UUID) (domain.Task, error) {
	t, err := scanTask(s.pool.QueryRow(ctx, `SELECT `+taskCols+` FROM tasks WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Task{}, domain.ErrTaskNotFound
	}
	return t, err
}

// ListTasks returns a page of tasks in id order, optionally filtered.
func (s *Store) ListTasks(ctx context.Context, f domain.TaskFilter) (domain.Page[domain.Task], error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+taskCols+` FROM tasks
		 WHERE ($1::uuid IS NULL OR tenant_id = $1)
		   AND ($2::text IS NULL OR status = $2)
		   AND ($3::text IS NULL OR type = $3)
		   AND ($4::uuid IS NULL OR id > $4)
		 ORDER BY id
		 LIMIT $5`,
		f.TenantID, optString(f.Status), optString(f.Type), f.Cursor, f.Limit+1)
	if err != nil {
		return domain.Page[domain.Task]{}, fmt.Errorf("list tasks: %w", err)
	}
	items, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Task, error) { return scanTask(r) })
	if err != nil {
		return domain.Page[domain.Task]{}, fmt.Errorf("list tasks: %w", err)
	}
	return page(items, f.Limit, func(t domain.Task) uuid.UUID { return t.ID }), nil
}

// ---- Helpers ----------------------------------------------------------------

// inTx runs fn in a transaction, committing on success and rolling back on
// any error (including domain errors such as version conflicts).
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// newID returns a UUIDv7: generated here (the task ID must exist before
// commit, since it doubles as the event ID) and time-ordered (index-friendly,
// and ORDER BY id is creation order for pagination).
func newID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

func scanTenant(row pgx.Row) (domain.Tenant, error) {
	var t domain.Tenant
	var status string
	err := row.Scan(&t.ID, &t.Slug, &t.Name, &status, &t.Version, &t.CreatedAt, &t.UpdatedAt)
	t.Status = domain.TenantStatus(status)
	t.CreatedAt, t.UpdatedAt = t.CreatedAt.UTC(), t.UpdatedAt.UTC()
	return t, err
}

func scanTask(row pgx.Row) (domain.Task, error) {
	var t domain.Task
	var typ, status string
	err := row.Scan(&t.ID, &typ, &t.TenantID, &status, &t.CreatedAt, &t.UpdatedAt)
	t.Type, t.Status = domain.TaskType(typ), domain.TaskStatus(status)
	t.CreatedAt, t.UpdatedAt = t.CreatedAt.UTC(), t.UpdatedAt.UTC()
	return t, err
}

// page trims the limit+1 probe row and sets the cursor if more rows exist.
func page[T any](items []T, limit int, idOf func(T) uuid.UUID) domain.Page[T] {
	p := domain.Page[T]{Items: items}
	if len(items) > limit {
		p.Items = items[:limit]
		next := idOf(items[limit-1])
		p.NextCursor = &next
	}
	if p.Items == nil {
		p.Items = []T{}
	}
	return p
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func statusStrings(ss []domain.TenantStatus) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}

func containsStatus(ss []domain.TenantStatus, s domain.TenantStatus) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// optString turns an optional string-typed filter into a nullable SQL param.
func optString[T ~string](p *T) *string {
	if p == nil {
		return nil
	}
	s := string(*p)
	return &s
}