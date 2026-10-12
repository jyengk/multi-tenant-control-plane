// Package domain holds the tenant and task state machines. It is pure Go with
// no I/O, so every rule here is unit-tested in isolation, and the store layer
// derives its conditional SQL (WHERE status = ANY(...)) from these tables
// instead of repeating them.
package domain

// TenantStatus is the lifecycle state of a tenant.
type TenantStatus string

// Tenant statuses.
const (
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantUpdating     TenantStatus = "updating"
	TenantDestroying   TenantStatus = "destroying"
	TenantDestroyed    TenantStatus = "destroyed"
	TenantFailed       TenantStatus = "failed"
)

// AllTenantStatuses lists every tenant status, used for validation and
// exhaustive tests.
var AllTenantStatuses = []TenantStatus{
	TenantProvisioning, TenantActive, TenantUpdating,
	TenantDestroying, TenantDestroyed, TenantFailed,
}

// Valid reports whether s is a known tenant status.
func (s TenantStatus) Valid() bool {
	for _, v := range AllTenantStatuses {
		if s == v {
			return true
		}
	}
	return false
}

// TaskStatus is the lifecycle state of a task.
type TaskStatus string

// Task statuses. Done and Failed are terminal.
const (
	TaskAccepted   TaskStatus = "accepted"
	TaskInProgress TaskStatus = "in_progress"
	TaskDone       TaskStatus = "done"
	TaskFailed     TaskStatus = "failed"
)

// AllTaskStatuses lists every task status.
var AllTaskStatuses = []TaskStatus{TaskAccepted, TaskInProgress, TaskDone, TaskFailed}

// Valid reports whether s is a known task status.
func (s TaskStatus) Valid() bool {
	for _, v := range AllTaskStatuses {
		if s == v {
			return true
		}
	}
	return false
}

// IsTerminal reports whether no further transitions are possible.
func (s TaskStatus) IsTerminal() bool { return s == TaskDone || s == TaskFailed }

// IsOpen reports whether the task is still being worked on.
func (s TaskStatus) IsOpen() bool { return s == TaskAccepted || s == TaskInProgress }

// TaskType is the kind of work a task represents.
type TaskType string

// Task types.
const (
	TaskDeploy  TaskType = "deploy"
	TaskUpdate  TaskType = "update"
	TaskDestroy TaskType = "destroy"
)

// AllTaskTypes lists every task type.
var AllTaskTypes = []TaskType{TaskDeploy, TaskUpdate, TaskDestroy}

// Valid reports whether t is a known task type.
func (t TaskType) Valid() bool {
	return t == TaskDeploy || t == TaskUpdate || t == TaskDestroy
}
