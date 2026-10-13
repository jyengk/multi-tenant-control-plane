// Package store is the Postgres layer. For now it only connects and applies
// the embedded schema; the repositories (tenants, tasks, processed updates)
// come later and will implement api.Service.
package store

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaSQL is compiled into the binary, so the image needs no extra files.
//
//go:embed schema.sql
var schemaSQL string

// schemaLockID is an arbitrary constant naming the advisory lock that
// serialises schema application across replicas starting at the same time.
const schemaLockID = 727_101

// Connect opens a connection pool and waits until Postgres answers, retrying
// with exponential backoff (500ms doubling to a 30s cap) until ctx is done.
func Connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		// Don't wrap err: it can echo the URL, which contains the password.
		return nil, fmt.Errorf("invalid DATABASE_URL")
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	backoff := 500 * time.Millisecond
	const maxBackoff = 30 * time.Second
	for attempt := 1; ; attempt++ {
		err := pool.Ping(ctx)
		if err == nil {
			slog.Info("connected to postgres", "attempt", attempt)
			return pool, nil
		}
		slog.Warn("postgres not reachable, retrying", "attempt", attempt, "retry_in", backoff.String(), "err", err)
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// ApplySchema runs schema.sql in one transaction. Every statement is
// IF NOT EXISTS, so this is safe on every startup. The transaction-scoped
// advisory lock stops two replicas from racing on the same CREATE, which can
// fail even with IF NOT EXISTS; the second simply waits, then finds
// everything already there.
func ApplySchema(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", schemaLockID); err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	if _, err := tx.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit schema: %w", err)
	}
	slog.Info("database schema applied")
	return nil
}

// Open is the startup sequence: connect (with retry), then apply the schema.
func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := ApplySchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}