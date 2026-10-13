// Package broker wraps RabbitMQ: connecting with retry, declaring the queue
// topology, publishing with publisher confirms, and consuming with manual
// acks. The outbox relay, the worker and the task update consumer all use it.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Queue names. The suffix ".dlq" marks the dead-letter queue for each.
const (
	QueueTasks       = "tasks"        // control-plane → worker
	QueueTaskUpdates = "task-updates" // worker → control-plane
	dlqSuffix        = ".dlq" // Dead letter queue
)

// DLQ returns the dead-letter queue name for queue.
func DLQ(queue string) string { return queue + dlqSuffix }

// queueSpec describes one queue we declare.
type queueSpec struct {
	Name string
	Args amqp.Table
}

// Topology lists every queue, dead-letter queues first so they exist before
// anything can be routed to them.
//
// Each main queue dead-letters through the default exchange ("") straight
// into its .dlq: a message rejected with requeue=false (a poison message)
// lands there instead of blocking the queue.
func Topology() []queueSpec {
	var specs []queueSpec
	for _, q := range []string{QueueTasks, QueueTaskUpdates} {
		specs = append(specs, queueSpec{Name: DLQ(q)})
	}
	for _, q := range []string{QueueTasks, QueueTaskUpdates} {
		specs = append(specs, queueSpec{Name: q, Args: amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": DLQ(q),
		}})
	}
	return specs
}

// DeclareTopology declares every queue as durable (survives broker
// restarts). Declaring is idempotent: an existing queue with the same
// settings is left alone, so every process can safely do this on startup.
func DeclareTopology(ch *amqp.Channel) error {
	for _, q := range Topology() {
		if _, err := ch.QueueDeclare(q.Name, true, false, false, false, q.Args); err != nil {
			return fmt.Errorf("declare queue %q: %w", q.Name, err)
		}
	}
	return nil
}

// Dial connects to RabbitMQ, retrying with exponential backoff (500ms
// doubling to a 30s cap) until it succeeds or ctx is cancelled. The broker
// often comes up after the app, so failing fast would be wrong here.
func Dial(ctx context.Context, url string) (*amqp.Connection, error) {
	backoff := 500 * time.Millisecond
	const maxBackoff = 30 * time.Second
	for attempt := 1; ; attempt++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			slog.Info("connected to rabbitmq", "attempt", attempt)
			return conn, nil
		}
		slog.Warn("rabbitmq not reachable, retrying", "attempt", attempt, "retry_in", backoff.String(), "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// Setup is the usual startup sequence: dial, open a channel, declare the
// topology. The caller closes the connection (which closes the channel).
func Setup(ctx context.Context, url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := Dial(ctx, url)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	if err := DeclareTopology(ch); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	slog.Info("rabbitmq topology declared", "queues", queueNames())
	return conn, ch, nil
}

func queueNames() []string {
	var names []string
	for _, q := range Topology() {
		names = append(names, q.Name)
	}
	return names
}

// ---- Publishing --------------------------------------------------------------

// Publisher publishes persistent JSON messages and waits for the broker to
// confirm each one, so "published" means "stored by RabbitMQ".
type Publisher struct {
	ch *amqp.Channel
}

// NewPublisher puts ch into confirm mode. Use a dedicated channel for it.
func NewPublisher(ch *amqp.Channel) (*Publisher, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	return &Publisher{ch: ch}, nil
}

// ErrNotConfirmed means the broker nacked the message: it was not stored.
var ErrNotConfirmed = errors.New("broker did not confirm message")

// Publish sends body to queue (via the default exchange) and blocks until the
// broker confirms it or ctx expires. messageID is the idempotency key the
// receiver dedupes on (task ID for tasks, update ID for updates).
func (p *Publisher) Publish(ctx context.Context, queue, messageID string, body []byte) error {
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("publish to %q: %w", queue, err)
	}
	ok, err := conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("await confirm: %w", err)
	}
	if !ok {
		return ErrNotConfirmed
	}
	return nil
}

// ---- Consuming ---------------------------------------------------------------

// Consume starts delivering messages from queue with manual acks. prefetch
// caps unacked messages in flight per consumer, so one slow consumer doesn't
// hoard the queue. The handler decides ack, nack (retry) or reject (→ DLQ).
func Consume(ch *amqp.Channel, queue, consumerTag string, prefetch int) (<-chan amqp.Delivery, error) {
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return nil, fmt.Errorf("set prefetch: %w", err)
	}
	deliveries, err := ch.Consume(queue, consumerTag, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("consume %q: %w", queue, err)
	}
	return deliveries, nil
}