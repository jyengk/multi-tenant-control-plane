package domain

import (
	"errors"
	"testing"
)

// TestApply_FullMatrix checks every (operation, tenant status) pair against
// the agreed matrix, so a status added later without a decision fails here.
func TestApply_FullMatrix(t *testing.T) {
	type want struct {
		to   TenantStatus
		task TaskType
	}
	// Only allowed pairs are listed; everything else must be not_allowed.
	allowed := map[Operation]map[TenantStatus]want{
		OpUpdate: {
			TenantActive: {TenantUpdating, TaskUpdate},
		},
		OpDelete: {
			TenantActive: {TenantDestroying, TaskDestroy},
			TenantFailed: {TenantDestroying, TaskDestroy},
		},
	}

	for _, op := range []Operation{OpUpdate, OpDelete} {
		for _, from := range AllTenantStatuses {
			t.Run(string(op)+"_from_"+string(from), func(t *testing.T) {
				to, task, err := Apply(op, from)
				w, ok := allowed[op][from]
				if ok {
					if err != nil {
						t.Fatalf("expected allowed, got %v", err)
					}
					if to != w.to || task != w.task {
						t.Fatalf("got (%s, %s), want (%s, %s)", to, task, w.to, w.task)
					}
					return
				}
				if !errors.Is(err, ErrTenantUpdateNotAllowed) {
					t.Fatalf("expected tenant_update_not_allowed, got (%s, %s, %v)", to, task, err)
				}
			})
		}
	}
}

func TestCreateTransition(t *testing.T) {
	tr := TransitionFor(OpCreate)
	if tr.To != TenantProvisioning || tr.TaskType != TaskDeploy {
		t.Fatalf("create: got %+v", tr)
	}
}

func TestTransitionFor_UnknownOpPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for unknown operation")
		}
	}()
	TransitionFor("bogus")
}

// TestCanTransitionTask_FullMatrix checks every (from, to) task pair:
// forward-only, terminal states are sinks, nothing goes back to accepted.
func TestCanTransitionTask_FullMatrix(t *testing.T) {
	allowed := map[[2]TaskStatus]bool{
		{TaskAccepted, TaskInProgress}: true,
		{TaskAccepted, TaskDone}:       true, // forward jump, see taskPredecessors
		{TaskAccepted, TaskFailed}:     true,
		{TaskInProgress, TaskDone}:     true,
		{TaskInProgress, TaskFailed}:   true,
	}
	for _, from := range AllTaskStatuses {
		for _, to := range AllTaskStatuses {
			got := CanTransitionTask(from, to)
			if want := allowed[[2]TaskStatus{from, to}]; got != want {
				t.Errorf("%s -> %s: got %v, want %v", from, to, got, want)
			}
		}
	}
}

// The stale-update scenarios the brief calls out by name.
func TestCanTransitionTask_StaleUpdatesRejected(t *testing.T) {
	cases := []struct{ from, to TaskStatus }{
		{TaskDone, TaskInProgress},   // in_progress after done
		{TaskFailed, TaskInProgress}, // in_progress after failed
		{TaskDone, TaskFailed},       // conflicting terminal
		{TaskFailed, TaskDone},
		{TaskDone, TaskDone}, // duplicate terminal
		{TaskInProgress, TaskInProgress},
		{TaskInProgress, TaskAccepted}, // regression
	}
	for _, c := range cases {
		if CanTransitionTask(c.from, c.to) {
			t.Errorf("%s -> %s should be rejected as stale", c.from, c.to)
		}
	}
}

func TestTaskPredecessors_MatchesCanTransition(t *testing.T) {
	for _, to := range AllTaskStatuses {
		for _, from := range TaskPredecessors(to) {
			if !CanTransitionTask(from, to) {
				t.Errorf("predecessor %s of %s not accepted by CanTransitionTask", from, to)
			}
		}
	}
	if len(TaskPredecessors(TaskAccepted)) != 0 {
		t.Error("nothing may transition into accepted")
	}
}

func TestTenantOnOutcome_FullMatrix(t *testing.T) {
	want := map[TaskType]map[TaskStatus]OutcomeTransition{
		TaskDeploy: {
			TaskDone:   {TenantProvisioning, TenantActive},
			TaskFailed: {TenantProvisioning, TenantFailed},
		},
		TaskUpdate: {
			TaskDone:   {TenantUpdating, TenantActive},
			TaskFailed: {TenantUpdating, TenantFailed},
		},
		TaskDestroy: {
			TaskDone:   {TenantDestroying, TenantDestroyed},
			TaskFailed: {TenantDestroying, TenantFailed},
		},
	}
	for _, tt := range AllTaskTypes {
		for _, st := range AllTaskStatuses {
			got, ok := TenantOnOutcome(tt, st)
			w, wantOK := want[tt][st]
			if ok != wantOK || got != w {
				t.Errorf("%s/%s: got (%+v, %v), want (%+v, %v)", tt, st, got, ok, w, wantOK)
			}
			if ok != st.IsTerminal() {
				t.Errorf("%s/%s: only terminal outcomes may move a tenant", tt, st)
			}
		}
	}
}

// Every outcome's guard status must be the status the creating operation
// put the tenant in, so the API and worker tables can't drift apart.
func TestOutcomeGuardsMatchOperationTargets(t *testing.T) {
	opFor := map[TaskType]Operation{TaskDeploy: OpCreate, TaskUpdate: OpUpdate, TaskDestroy: OpDelete}
	for tt, op := range opFor {
		target := TransitionFor(op).To
		for _, st := range []TaskStatus{TaskDone, TaskFailed} {
			tr, _ := TenantOnOutcome(tt, st)
			if tr.From != target {
				t.Errorf("%s/%s guard is %s, but %s leaves tenant in %s", tt, st, tr.From, op, target)
			}
		}
	}
}

func TestStatusPredicates(t *testing.T) {
	for _, s := range AllTaskStatuses {
		if s.IsOpen() == s.IsTerminal() {
			t.Errorf("%s must be exactly one of open or terminal", s)
		}
	}
	if TenantStatus("deleted").Valid() || TaskStatus("queued").Valid() || TaskType("migrate").Valid() {
		t.Error("unknown values must be invalid")
	}
}
