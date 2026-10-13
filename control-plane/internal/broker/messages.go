package broker

import (
	"time"

	"github.com/google/uuid"
)

// TaskMessage is the event published to the "tasks" queue. The task IS the
// event: a consumer recovers the task's id, type, tenant_id and status from
// it. AMQP message_id is set to ID, so consumers can dedupe on it.
type TaskMessage struct {
	ID        uuid.UUID `json:"id"`
	Type      string    `json:"type"`
	TenantID  uuid.UUID `json:"tenant_id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// TaskUpdateMessage is a progress report on the "task-updates" queue,
// published by the worker and applied by the task update consumer.
//
//   - UpdateID is the idempotency key: the consumer records it, so the same
//     update delivered twice changes nothing. AMQP message_id is set to it.
//   - TaskID says which task it updates.
//   - Status is in_progress, done or failed.
type TaskUpdateMessage struct {
	UpdateID   uuid.UUID `json:"update_id"`
	TaskID     uuid.UUID `json:"task_id"`
	Status     string    `json:"status"`
	OccurredAt time.Time `json:"occurred_at"`
}