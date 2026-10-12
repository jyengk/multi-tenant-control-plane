package domain

// ---------------------------------------------------------------------------
// Tenant transitions driven by the HTTP API
// ---------------------------------------------------------------------------

// Operation is a state-changing tenant API call.
type Operation string

// Tenant API operations. Create is listed for completeness: it has no
// "from" status because the tenant does not exist yet.
const (
	OpCreate Operation = "create"
	OpUpdate Operation = "update" // PATCH
	OpDelete Operation = "delete" // DELETE
)

// OpTransition describes what an API operation does to a tenant.
type OpTransition struct {
	From     []TenantStatus // statuses the operation is allowed from (empty for create)
	To       TenantStatus   // status the tenant moves to
	TaskType TaskType       // type of the task the operation creates
}

// opTransitions is the authoritative table for API-driven tenant changes.
//
//	create:                 → provisioning  (+ deploy task)
//	update: active          → updating      (+ update task)
//	delete: active | failed → destroying    (+ destroy task)
var opTransitions = map[Operation]OpTransition{
	OpCreate: {From: nil, To: TenantProvisioning, TaskType: TaskDeploy},
	OpUpdate: {From: []TenantStatus{TenantActive}, To: TenantUpdating, TaskType: TaskUpdate},
	OpDelete: {From: []TenantStatus{TenantActive, TenantFailed}, To: TenantDestroying, TaskType: TaskDestroy},
}

// TransitionFor returns the transition for op. It panics on an unknown
// operation, which is a programming error, not a runtime condition.
func TransitionFor(op Operation) OpTransition {
	t, ok := opTransitions[op]
	if !ok {
		panic("domain: unknown operation " + string(op))
	}
	return t
}

// Apply checks whether op is allowed from current and returns the resulting
// tenant status and the type of task to create. A disallowed transition
// returns ErrTenantUpdateNotAllowed; the same code covers PATCH and DELETE
// because the brief's error codes are a fixed contract.
func Apply(op Operation, current TenantStatus) (TenantStatus, TaskType, error) {
	t := TransitionFor(op)
	for _, from := range t.From {
		if current == from {
			return t.To, t.TaskType, nil
		}
	}
	return "", "", ErrTenantUpdateNotAllowed
}

// ---------------------------------------------------------------------------
// Task transitions driven by worker updates
// ---------------------------------------------------------------------------

// taskPredecessors maps a target task status to the statuses it may be
// reached from. Transitions are forward-only. Terminal outcomes may be
// reached directly from accepted (design decision: if done arrives before
// in_progress, the work did finish, and rejecting it would strand the task
// forever).
var taskPredecessors = map[TaskStatus][]TaskStatus{
	TaskAccepted:   nil, // only ever set at creation
	TaskInProgress: {TaskAccepted},
	TaskDone:       {TaskAccepted, TaskInProgress},
	TaskFailed:     {TaskAccepted, TaskInProgress},
}

// TaskPredecessors returns the statuses a task may move to target from. The
// store uses it as the guard in `UPDATE tasks ... WHERE status = ANY($n)`, so
// a stale or duplicate update simply matches zero rows.
func TaskPredecessors(target TaskStatus) []TaskStatus {
	return taskPredecessors[target]
}

// CanTransitionTask reports whether a task may move from -> to. Anything not
// allowed (backwards, sideways, or out of a terminal state) is a stale
// update that must be ignored, never an error.
func CanTransitionTask(from, to TaskStatus) bool {
	for _, p := range taskPredecessors[to] {
		if p == from {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Tenant transitions driven by terminal task outcomes
// ---------------------------------------------------------------------------

// OutcomeTransition describes how a terminal task outcome moves the tenant.
type OutcomeTransition struct {
	From TenantStatus // status the tenant must be in (defensive guard)
	To   TenantStatus
}

// outcomeTransitions is the authoritative table for worker-driven tenant
// changes. Only terminal outcomes appear: in_progress never moves a tenant.
//
//	deploy  done → active     | failed → failed   (from provisioning)
//	update  done → active     | failed → failed   (from updating)
//	destroy done → destroyed  | failed → failed   (from destroying)
var outcomeTransitions = map[TaskType]map[TaskStatus]OutcomeTransition{
	TaskDeploy: {
		TaskDone:   {From: TenantProvisioning, To: TenantActive},
		TaskFailed: {From: TenantProvisioning, To: TenantFailed},
	},
	TaskUpdate: {
		TaskDone:   {From: TenantUpdating, To: TenantActive},
		TaskFailed: {From: TenantUpdating, To: TenantFailed},
	},
	TaskDestroy: {
		TaskDone:   {From: TenantDestroying, To: TenantDestroyed},
		TaskFailed: {From: TenantDestroying, To: TenantFailed},
	},
}

// TenantOnOutcome returns how a task of type tt reaching status outcome moves
// its tenant. ok is false for non-terminal statuses, which leave the tenant
// unchanged.
func TenantOnOutcome(tt TaskType, outcome TaskStatus) (OutcomeTransition, bool) {
	t, ok := outcomeTransitions[tt][outcome]
	return t, ok
}
