package api

import (
	"time"

	"controlplane/internal/domain"
)

// timeLayout is ISO 8601 with milliseconds and a Z suffix, as the brief requires.
const timeLayout = "2006-01-02T15:04:05.000Z"

func formatTime(t time.Time) string { return t.UTC().Format(timeLayout) }

// ---- Requests --------------------------------------------------------------

type createTenantRequest struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

// updateTenantRequest uses pointers so we can tell "absent" from "zero".
// Slug is declared only so we can reject it with a clear message instead of
// a generic "unknown field": slugs are immutable.
type updateTenantRequest struct {
	Version *int64  `json:"version"`
	Name    *string `json:"name"`
	Slug    *string `json:"slug"`
}

// ---- Responses -------------------------------------------------------------

type tenantResponse struct {
	ID        string `json:"id"`
	Slug      string `json:"slug"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Version   int64  `json:"version"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type taskResponse struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	TenantID  string `json:"tenant_id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// mutationResponse is returned by POST, PATCH and DELETE on tenants: the
// tenant's new state plus the task that will carry out the change.
type mutationResponse struct {
	Tenant tenantResponse `json:"tenant"`
	Task   taskResponse   `json:"task"`
}

type listResponse[T any] struct {
	Data       []T     `json:"data"`
	NextCursor *string `json:"next_cursor"`
}

func toTenantResponse(t domain.Tenant) tenantResponse {
	return tenantResponse{
		ID:        t.ID.String(),
		Slug:      t.Slug,
		Name:      t.Name,
		Status:    string(t.Status),
		Version:   t.Version,
		CreatedAt: formatTime(t.CreatedAt),
		UpdatedAt: formatTime(t.UpdatedAt),
	}
}

func toTaskResponse(t domain.Task) taskResponse {
	return taskResponse{
		ID:        t.ID.String(),
		Type:      string(t.Type),
		TenantID:  t.TenantID.String(),
		Status:    string(t.Status),
		CreatedAt: formatTime(t.CreatedAt),
		UpdatedAt: formatTime(t.UpdatedAt),
	}
}

func toListResponse[D, R any](p domain.Page[D], conv func(D) R) listResponse[R] {
	out := listResponse[R]{Data: make([]R, 0, len(p.Items))}
	for _, item := range p.Items {
		out.Data = append(out.Data, conv(item))
	}
	if p.NextCursor != nil {
		s := p.NextCursor.String()
		out.NextCursor = &s
	}
	return out
}
