// Package consumer is the task update consumer: it reads worker progress
// updates from the "task-updates" queue and applies them to Postgres.
//
// Delivery is at-least-once, so the consumer is:
//   - idempotent: each update_id is applied at most once (store.ApplyTaskUpdate)
//   - order-tolerant: updates that would move a task backwards are ignored
//   - resilient: transient failures are retried with backoff; poison messages
//     go to the dead-letter queue and never block or crash the consumer
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"controlplane/internal/broker"
	"controlplane/internal/domain"
	"controlplane/internal/store"
)

// Applier applies one update. *store.Store implements it; tests use a fake.
type Applier interface {
	ApplyTaskUpdate(ctx context.Context, u store.TaskUpdate) (store.UpdateOutcome, error)
}

// Consumer consumes "task-updates".
type Consumer struct {
	applier  Applier
	amqpURL  string
	prefetch int
	retryMin time.Duration // backoff after a transient failure...
	retryMax time.Duration // ...doubling up to this
}

// New returns a consumer. Call Run to start it.
func New(applier Applier, amqpURL string) *Consumer {
	return &Consumer{
		applier:  applier,
		amqpURL:  amqpURL,
		prefetch: 20,
		retryMin: 200 * time.Millisecond,
		retryMax: 10 * time.Second,
	}
}

// Run consumes until ctx is cancelled, reconnecting when the broker drops.
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := c.session(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("task update consumer: session ended, reconnecting", "err", err)
			sleep(ctx, time.Second)
		}
	}
}

func (c *Consumer) session(ctx context.Context) error {
	conn, ch, err := broker.Setup(ctx, c.amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	deliveries, err := broker.Consume(ch, broker.QueueTaskUpdates, "control-plane", c.prefetch)
	if err != nil {
		return err
	}
	slog.Info("task update consumer started", "queue", broker.QueueTaskUpdates)

	backoff := c.retryMin
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed (connection lost)")
			}
			if c.handle(ctx, d) {
				backoff = c.retryMin
				continue
			}
			// Transient failure: wait before taking the next message, so a
			// database outage doesn't become a hot redelivery loop.
			sleep(ctx, backoff)
			backoff = min(backoff*2, c.retryMax)
		}
	}
}

// handle processes one delivery and acks, rejects or nacks it. It returns
// false only for transient failures (the message was requeued).
//
//	malformed / invalid / unknown task → reject (no requeue) → task-updates.dlq
//	applied / stale / duplicate        → ack
//	any other error (DB down, ...)     → nack with requeue → retried later
//
// Ack happens only AFTER the database commit, so a crash in between causes a
// redelivery, which the idempotency check turns into a no-op.
func (c *Consumer) handle(ctx context.Context, d amqp.Delivery) bool {
	u, err := decode(d.Body)
	if err != nil {
		slog.Error("task update consumer: poison message, dead-lettering",
			"message_id", d.MessageId, "err", err)
		_ = d.Reject(false)
		return true
	}
	log := slog.With("update_id", u.UpdateID, "task_id", u.TaskID, "status", u.Status)

	outcome, err := c.applier.ApplyTaskUpdate(ctx, u)
	switch {
	case errors.Is(err, store.ErrUnknownTask):
		log.Error("task update consumer: unknown task, dead-lettering")
		_ = d.Reject(false)
		return true
	case err != nil:
		log.Warn("task update consumer: apply failed, will retry", "err", err)
		_ = d.Nack(false, true)
		return false
	}

	log.Info("task update consumer: processed", "outcome", outcome)
	_ = d.Ack(false)
	return true
}

// decode parses and validates an update. Anything that can never succeed is
// an error here (→ dead letter), not a retry.
func decode(body []byte) (store.TaskUpdate, error) {
	var m broker.TaskUpdateMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return store.TaskUpdate{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if m.UpdateID == uuid.Nil {
		return store.TaskUpdate{}, errors.New("missing update_id")
	}
	if m.TaskID == uuid.Nil {
		return store.TaskUpdate{}, errors.New("missing task_id")
	}
	st := domain.TaskStatus(m.Status)
	// Workers report progress only; "accepted" is set by the control plane.
	if st != domain.TaskInProgress && st != domain.TaskDone && st != domain.TaskFailed {
		return store.TaskUpdate{}, fmt.Errorf("invalid status %q", m.Status)
	}
	return store.TaskUpdate{UpdateID: m.UpdateID, TaskID: m.TaskID, Status: st}, nil
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}