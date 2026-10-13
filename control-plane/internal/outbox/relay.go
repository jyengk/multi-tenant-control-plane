// Package outbox implements the transactional-outbox relay: it publishes
// committed tasks to RabbitMQ and marks them published.
//
// Why: the API must never publish before its transaction commits (or after a
// rollback), and must never lose a task if the broker is down or the process
// crashes. So the API only writes the task row (published_at NULL = "in the
// outbox"), and this relay, running separately, does the publishing.
//
// Guarantees:
//   - only committed rows are visible to the relay → never publish before commit
//   - a row stays NULL until RabbitMQ confirms it → never lost
//   - a crash after publish but before marking → published again (at-least-once;
//     consumers dedupe on the task id)
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"controlplane/internal/broker"
)

// Config tunes the relay. Zero values get sensible defaults.
type Config struct {
	PollInterval   time.Duration // safety-net poll; the nudge handles the fast path
	BatchSize      int           // max tasks claimed per round
	PublishTimeout time.Duration // per batch; bounds how long row locks are held
}

func (c *Config) defaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 5 * time.Second
	}
}

// Relay moves tasks from the outbox (published_at IS NULL) to the broker.
type Relay struct {
	pool    *pgxpool.Pool
	amqpURL string
	cfg     Config
	nudge   chan struct{}
}

// New creates a relay. Call Run to start it.
func New(pool *pgxpool.Pool, amqpURL string, cfg Config) *Relay {
	cfg.defaults()
	return &Relay{
		pool:    pool,
		amqpURL: amqpURL,
		cfg:     cfg,
		nudge:   make(chan struct{}, 1), // buffered: one pending wake-up is enough
	}
}

// Nudge wakes the relay immediately (called after the API commits a task).
// Non-blocking: if a wake-up is already pending, this one is dropped, which
// is fine because the pending one will drain everything anyway.
func (r *Relay) Nudge() {
	select {
	case r.nudge <- struct{}{}:
	default:
	}
}

// Run publishes until ctx is cancelled. It (re)connects to RabbitMQ with
// backoff, so a broker outage pauses publishing without affecting the API:
// tasks simply accumulate in the outbox and drain when the broker returns.
func (r *Relay) Run(ctx context.Context) {
	for ctx.Err() == nil {
		conn, ch, err := broker.Setup(ctx, r.amqpURL)
		if err != nil {
			return // only fails when ctx is cancelled
		}
		pub, err := broker.NewPublisher(ch)
		if err != nil {
			slog.Error("outbox relay: publisher setup failed", "err", err)
			_ = conn.Close()
			sleep(ctx, time.Second)
			continue
		}
		slog.Info("outbox relay started", "batch_size", r.cfg.BatchSize, "poll_interval", r.cfg.PollInterval.String())

		r.loop(ctx, pub, conn.NotifyClose(make(chan *amqp.Error, 1)))
		_ = conn.Close()
	}
}

// loop drains the outbox on every tick or nudge, until ctx is cancelled or
// the broker connection drops (then Run reconnects).
func (r *Relay) loop(ctx context.Context, pub *broker.Publisher, closed <-chan *amqp.Error) {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-closed:
			slog.Warn("outbox relay: broker connection lost, reconnecting", "err", err)
			return
		case <-ticker.C:
		case <-r.nudge:
		}
		if err := r.drain(ctx, pub); err != nil && ctx.Err() == nil {
			slog.Error("outbox relay: publish round failed; will retry", "err", err)
		}
	}
}

// drain publishes batches until the outbox is empty (or a batch fails).
func (r *Relay) drain(ctx context.Context, pub *broker.Publisher) error {
	for {
		n, err := r.publishBatch(ctx, pub)
		if err != nil {
			return err
		}
		if n < r.cfg.BatchSize {
			return nil // short batch: nothing left right now
		}
	}
}

// publishBatch claims up to BatchSize unpublished tasks in creation order,
// publishes each (waiting for the broker's confirm), and marks the confirmed
// ones published, all in one transaction.
//
// FOR UPDATE SKIP LOCKED lets several relays (replicas) run safely: each
// claims different rows and nobody waits on anybody else's lock. If a
// publish fails partway, the ones already confirmed are still marked, and
// the rest stay NULL for the next round.
func (r *Relay) publishBatch(ctx context.Context, pub *broker.Publisher) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	rows, err := tx.Query(ctx, `
		SELECT id, type, tenant_id, status, created_at
		  FROM tasks
		 WHERE published_at IS NULL
		 ORDER BY id
		 LIMIT $1
		   FOR UPDATE SKIP LOCKED`, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("claim tasks: %w", err)
	}
	batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (broker.TaskMessage, error) {
		var m broker.TaskMessage
		err := row.Scan(&m.ID, &m.Type, &m.TenantID, &m.Status, &m.CreatedAt)
		m.CreatedAt = m.CreatedAt.UTC()
		return m, err
	})
	if err != nil {
		return 0, fmt.Errorf("read tasks: %w", err)
	}
	if len(batch) == 0 {
		return 0, nil
	}

	// Publish in creation order; stop at the first failure.
	published := make([]uuid.UUID, 0, len(batch))
	var pubErr error
	for _, m := range batch {
		body, err := json.Marshal(m)
		if err != nil {
			pubErr = fmt.Errorf("encode task %s: %w", m.ID, err)
			break
		}
		if err := pub.Publish(ctx, broker.QueueTasks, m.ID.String(), body); err != nil {
			pubErr = fmt.Errorf("publish task %s: %w", m.ID, err)
			break
		}
		published = append(published, m.ID)
	}

	if len(published) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE tasks SET published_at = now() WHERE id = ANY($1)`, published); err != nil {
			// Not marked → they'll be published again next round (duplicate,
			// which consumers tolerate). Never the other way round.
			return 0, errors.Join(pubErr, fmt.Errorf("mark published: %w", err))
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, errors.Join(pubErr, fmt.Errorf("commit: %w", err))
		}
		slog.Info("outbox relay: published tasks", "count", len(published))
	}
	if pubErr != nil {
		return len(published), pubErr
	}
	return len(batch), nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}