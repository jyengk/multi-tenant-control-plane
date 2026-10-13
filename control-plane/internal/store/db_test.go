package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool connects to TEST_DATABASE_URL, or skips the test when it's unset
// so `go test ./...` still passes without a database. Tables are dropped
// first so each test starts from an empty database.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS processed_messages, tasks, tenants CASCADE"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	return pool
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestApplySchema_IdempotentAndConcurrent(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	// Sequentially, twice.
	for i := 0; i < 2; i++ {
		if err := ApplySchema(ctx, pool); err != nil {
			t.Fatalf("apply #%d: %v", i+1, err)
		}
	}

	// Concurrently, as replicas booting together would.
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- ApplySchema(ctx, pool)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent apply: %v", err)
		}
	}

	var n int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('tenants','tasks','processed_messages')`).Scan(&n)
	if err != nil || n != 3 {
		t.Fatalf("expected 3 tables, got %d (%v)", n, err)
	}
}

func TestSchemaConstraints(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := ApplySchema(ctx, pool); err != nil {
		t.Fatal(err)
	}

	insertTenant := func(id, slug, name string) error {
		_, err := pool.Exec(ctx, `INSERT INTO tenants (id, slug, name, status, created_at, updated_at)
			VALUES ($1, $2, $3, 'provisioning', now(), now())`, id, slug, name)
		return err
	}
	const t1 = "0192f3a1-7c2e-7b4d-9f10-000000000001"

	if err := insertTenant(t1, "acme-eu", "Acme"); err != nil {
		t.Fatalf("valid insert: %v", err)
	}
	// 23505 = unique_violation: duplicate slug.
	if err := insertTenant("0192f3a1-7c2e-7b4d-9f10-000000000002", "acme-eu", "Other"); sqlState(err) != "23505" {
		t.Errorf("duplicate slug: want 23505, got %v", err)
	}
	// 23514 = check_violation: bad slug, blank name.
	if err := insertTenant("0192f3a1-7c2e-7b4d-9f10-000000000003", "Bad_Slug", "x"); sqlState(err) != "23514" {
		t.Errorf("bad slug: want 23514, got %v", err)
	}
	if err := insertTenant("0192f3a1-7c2e-7b4d-9f10-000000000004", "blank-name", "  "); sqlState(err) != "23514" {
		t.Errorf("blank name: want 23514, got %v", err)
	}

	insertTask := func(id, status string) error {
		_, err := pool.Exec(ctx, `INSERT INTO tasks (id, type, tenant_id, status, created_at, updated_at)
			VALUES ($1, 'deploy', $2, $3, now(), now())`, id, t1, status)
		return err
	}
	if err := insertTask("0192f3a1-7c2f-7000-8000-000000000001", "accepted"); err != nil {
		t.Fatalf("first open task: %v", err)
	}
	// A second open task for the same tenant must be impossible.
	if err := insertTask("0192f3a1-7c2f-7000-8000-000000000002", "in_progress"); sqlState(err) != "23505" {
		t.Errorf("second open task: want 23505, got %v", err)
	}
	// Closed tasks don't count toward the limit.
	if err := insertTask("0192f3a1-7c2f-7000-8000-000000000003", "done"); err != nil {
		t.Errorf("closed task: %v", err)
	}
	// Unknown task status.
	if err := insertTask("0192f3a1-7c2f-7000-8000-000000000004", "queued"); sqlState(err) != "23514" {
		t.Errorf("bad task status: want 23514, got %v", err)
	}
}