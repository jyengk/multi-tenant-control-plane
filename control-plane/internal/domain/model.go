package domain

import (
	"time"

	"github.com/google/uuid"
)

// Tenant is a customer environment, mutated through tasks.
type Tenant struct {
	ID        uuid.UUID
	Slug      string
	Name      string
	Status    TenantStatus
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Task is an asynchronous operation on a tenant. A task is the published
// event, persisted: its ID doubles as the event ID.
type Task struct {
	ID        uuid.UUID
	Type      TaskType
	TenantID  uuid.UUID
	Status    TaskStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CreateTenantInput is a validated create request.
type CreateTenantInput struct {
	Slug string
	Name string
}

// UpdateTenantInput is a validated partial update. Version is the client's
// last-seen version (optimistic locking); nil fields are left unchanged.
type UpdateTenantInput struct {
	Version int64
	Name    *string
}

// Pagination limits shared by all list endpoints.
const (
	DefaultPageLimit = 20
	MaxPageLimit     = 100
)

// TenantFilter selects a page of tenants. Results are ordered by ID
// (UUIDv7, so creation order); Cursor is the last ID of the previous page.
type TenantFilter struct {
	Status *TenantStatus
	Cursor *uuid.UUID
	Limit  int
}

// TaskFilter selects a page of tasks, ordered by ID.
type TaskFilter struct {
	TenantID *uuid.UUID
	Status   *TaskStatus
	Type     *TaskType
	Cursor   *uuid.UUID
	Limit    int
}

// Page is one page of results. NextCursor is nil on the last page.
type Page[T any] struct {
	Items      []T
	NextCursor *uuid.UUID
}
