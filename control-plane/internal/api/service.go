// Package api is the HTTP layer: routing, request decoding and validation,
// response encoding, and mapping domain errors to HTTP statuses. It holds no
// business rules; those live in domain and are enforced by the store.
package api

import (
	"context"

	"github.com/google/uuid"

	"controlplane/internal/domain"
)

// Service is everything the handlers need. The store package implements it;
// handler tests use a fake. Each mutating method writes the tenant and its
// new task in one transaction and returns both. Errors are *domain.Error
// values (tenant_not_found, tenant_already_exists, ...) or unexpected
// failures, which the API reports as 500 without leaking details.
type Service interface {
	CreateTenant(ctx context.Context, in domain.CreateTenantInput) (domain.Tenant, domain.Task, error)
	ListTenants(ctx context.Context, f domain.TenantFilter) (domain.Page[domain.Tenant], error)
	GetTenant(ctx context.Context, id uuid.UUID) (domain.Tenant, error)
	UpdateTenant(ctx context.Context, id uuid.UUID, in domain.UpdateTenantInput) (domain.Tenant, domain.Task, error)
	DeleteTenant(ctx context.Context, id uuid.UUID) (domain.Tenant, domain.Task, error)

	ListTasks(ctx context.Context, f domain.TaskFilter) (domain.Page[domain.Task], error)
	GetTask(ctx context.Context, id uuid.UUID) (domain.Task, error)
}
