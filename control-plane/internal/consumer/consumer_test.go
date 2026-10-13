package consumer

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"controlplane/internal/store"
)

// fakeAck records what the consumer did with a delivery.
type fakeAck struct{ action string }

func (f *fakeAck) Ack(uint64, bool) error { f.action = "ack"; return nil }
func (f *fakeAck) Nack(_ uint64, _ bool, requeue bool) error {
	f.action = map[bool]string{true: "nack-requeue", false: "nack"}[requeue]
	return nil
}
func (f *fakeAck) Reject(_ uint64, requeue bool) error {
	f.action = map[bool]string{true: "reject-requeue", false: "reject"}[requeue]
	return nil
}

type fakeApplier struct {
	outcome store.UpdateOutcome
	err     error
	called  bool
}

func (f *fakeApplier) ApplyTaskUpdate(context.Context, store.TaskUpdate) (store.UpdateOutcome, error) {
	f.called = true
	return f.outcome, f.err
}

const validBody = `{"update_id":"01a13683-c4cf-7809-a22c-f49ea6feb6f5",` +
	`"task_id":"01a1365c-a96e-7484-8249-9d63e8bf815e","status":"done",` +
	`"occurred_at":"2026-10-13T18:26:10.520Z"}`

func TestHandle(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		applier    *fakeApplier
		wantAction string
		wantOK     bool
		wantApply  bool
	}{
		{"applied → ack", validBody, &fakeApplier{outcome: store.OutcomeApplied}, "ack", true, true},
		{"stale → ack", validBody, &fakeApplier{outcome: store.OutcomeStaleIgnored}, "ack", true, true},
		{"duplicate → ack", validBody, &fakeApplier{outcome: store.OutcomeDuplicate}, "ack", true, true},
		{"unknown task → dead-letter", validBody, &fakeApplier{err: store.ErrUnknownTask}, "reject", true, true},
		{"db error → requeue for retry", validBody, &fakeApplier{err: errors.New("connection refused")}, "nack-requeue", false, true},

		// Poison messages never reach the database.
		{"not JSON", `garbage`, &fakeApplier{}, "reject", true, false},
		{"missing update_id", `{"task_id":"01a1365c-a96e-7484-8249-9d63e8bf815e","status":"done"}`, &fakeApplier{}, "reject", true, false},
		{"missing task_id", `{"update_id":"01a13683-c4cf-7809-a22c-f49ea6feb6f5","status":"done"}`, &fakeApplier{}, "reject", true, false},
		{"bad status", `{"update_id":"01a13683-c4cf-7809-a22c-f49ea6feb6f5","task_id":"01a1365c-a96e-7484-8249-9d63e8bf815e","status":"exploded"}`, &fakeApplier{}, "reject", true, false},
		{"accepted is not a worker status", `{"update_id":"01a13683-c4cf-7809-a22c-f49ea6feb6f5","task_id":"01a1365c-a96e-7484-8249-9d63e8bf815e","status":"accepted"}`, &fakeApplier{}, "reject", true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ack := &fakeAck{}
			d := amqp.Delivery{Acknowledger: ack, Body: []byte(c.body)}
			ok := New(c.applier, "").handle(context.Background(), d)

			if ack.action != c.wantAction {
				t.Errorf("action: got %q, want %q", ack.action, c.wantAction)
			}
			if ok != c.wantOK {
				t.Errorf("ok: got %v, want %v", ok, c.wantOK)
			}
			if c.applier.called != c.wantApply {
				t.Errorf("applier called: got %v, want %v", c.applier.called, c.wantApply)
			}
		})
	}
}