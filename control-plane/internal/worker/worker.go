// Package worker simulates provisioning. It consumes tasks from the "tasks"
// queue and, for each one, reports progress on "task-updates":
//
//	in_progress → (random delay) → done | failed (by fail rate)
//
// It never touches the database: the control plane is the only writer, and
// learns about progress only through these messages.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"controlplane/internal/broker"
	"controlplane/internal/domain"
)

// Config controls the simulation. Set at startup (flags / env vars), so
// failure and delay scenarios are triggered by restarting with new values.
type Config struct {
	AMQPURL     string
	FailRate    float64       // 0..1: probability a task ends in failed
	MinDelay    time.Duration // simulated work, per task
	MaxDelay    time.Duration
	Concurrency int // tasks processed at once (= RabbitMQ prefetch)
}

// Worker consumes tasks and publishes progress updates.
type Worker struct {
	cfg Config
	rnd func() float64 // [0,1), for the fail-rate draw; swappable in tests
	dur func() time.Duration
}

// New returns a worker. Concurrency defaults to 10.
func New(cfg Config) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	w := &Worker{cfg: cfg, rnd: rand.Float64} //nolint:gosec // simulation, not security
	w.dur = func() time.Duration { return randomDelay(cfg.MinDelay, cfg.MaxDelay) }
	return w
}

// Run consumes until ctx is cancelled, reconnecting (with backoff, inside
// broker.Setup) whenever the RabbitMQ connection drops.
func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := w.session(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("worker: session ended, reconnecting", "err", err)
			sleep(ctx, time.Second)
		}
	}
}

// session is one broker connection: consume until the connection drops or
// ctx is cancelled, then wait for in-flight tasks to finish.
func (w *Worker) session(ctx context.Context) error {
	conn, consumeCh, err := broker.Setup(ctx, w.cfg.AMQPURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	// A separate channel for publishing: confirm mode and consuming on one
	// channel are allowed, but separate channels keep the flows independent.
	pubCh, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open publish channel: %w", err)
	}
	pub, err := broker.NewPublisher(pubCh)
	if err != nil {
		return err
	}

	deliveries, err := broker.Consume(consumeCh, broker.QueueTasks, "worker", w.cfg.Concurrency)
	if err != nil {
		return err
	}
	slog.Info("worker consuming", "queue", broker.QueueTasks, "concurrency", w.cfg.Concurrency,
		"fail_rate", w.cfg.FailRate, "min_delay", w.cfg.MinDelay.String(), "max_delay", w.cfg.MaxDelay.String())

	var inflight sync.WaitGroup
	defer inflight.Wait() // don't close the connection under a running task
	var pubMu sync.Mutex  // one publish at a time on the shared channel

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed (connection lost)")
			}
			// Prefetch caps unacked deliveries at Concurrency, so at most that
			// many of these goroutines exist at once.
			inflight.Add(1)
			go func() {
				defer inflight.Done()
				w.handle(ctx, d, func(m broker.TaskUpdateMessage) error {
					pubMu.Lock()
					defer pubMu.Unlock()
					return publishUpdate(ctx, pub, m)
				})
			}()
		}
	}
}

// handle processes one task delivery end to end. Ack/nack/reject rules:
//   - malformed message        → reject, no requeue → dead-letter queue
//   - publish failed / shutdown → nack with requeue → redelivered later
//   - both updates published   → ack
//
// A redelivered task is simply processed again. Its new updates have new
// update IDs, and the consumer's forward-only rule ignores any that would
// move a task backwards, so reprocessing is harmless.
func (w *Worker) handle(ctx context.Context, d amqp.Delivery, publish func(broker.TaskUpdateMessage) error) {
	var task broker.TaskMessage
	if err := json.Unmarshal(d.Body, &task); err != nil || task.ID == uuid.Nil {
		slog.Error("worker: poison message, dead-lettering", "message_id", d.MessageId, "err", err)
		_ = d.Reject(false)
		return
	}
	log := slog.With("task_id", task.ID, "type", task.Type, "tenant_id", task.TenantID)
	log.Info("worker: task received", "redelivered", d.Redelivered)

	steps := w.plan()
	for i, st := range steps {
		if i > 0 { // simulated work happens between in_progress and the outcome
			if !sleep(ctx, w.dur()) {
				log.Info("worker: shutting down mid-task, requeueing")
				_ = d.Nack(false, true)
				return
			}
		}
		m := broker.TaskUpdateMessage{
			UpdateID:   uuid.Must(uuid.NewV7()),
			TaskID:     task.ID,
			Status:     string(st),
			OccurredAt: time.Now().UTC().Truncate(time.Millisecond),
		}
		if err := publish(m); err != nil {
			log.Error("worker: publish update failed, requeueing task", "status", st, "err", err)
			_ = d.Nack(false, true)
			return
		}
		log.Info("worker: progress reported", "status", st, "update_id", m.UpdateID)
	}
	_ = d.Ack(false)
}

// plan decides the sequence of statuses to report: in_progress, then done or
// failed according to the fail rate.
func (w *Worker) plan() []domain.TaskStatus {
	outcome := domain.TaskDone
	if w.rnd() < w.cfg.FailRate {
		outcome = domain.TaskFailed
	}
	return []domain.TaskStatus{domain.TaskInProgress, outcome}
}

func publishUpdate(ctx context.Context, pub *broker.Publisher, m broker.TaskUpdateMessage) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return pub.Publish(ctx, broker.QueueTaskUpdates, m.UpdateID.String(), body)
}

// randomDelay returns a duration uniformly in [min, max].
func randomDelay(minD, maxD time.Duration) time.Duration {
	if maxD <= minD {
		return minD
	}
	return minD + time.Duration(rand.Int64N(int64(maxD-minD)+1)) //nolint:gosec // simulation
}

// sleep waits d or until ctx is cancelled; it reports whether it slept fully.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}