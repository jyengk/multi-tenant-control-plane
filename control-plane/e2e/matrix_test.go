//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// Exhaustive transition matrices, run against the live system. The test
// plays the worker (worker STOPPED), so it can hold a tenant in transient
// states like "updating" for as long as it needs.

// T1: every API operation × every tenant status. 3 cells are allowed; the
// other 9 must return 409 tenant_update_not_allowed AND change nothing (same
// version, no new task).
func TestMatrix_TenantOperations(t *testing.T) {
	requireNoWorker(t)

	allowed := map[string]map[string]string{ // op → from-status → resulting status
		"PATCH":  {"active": "updating"},
		"DELETE": {"active": "destroying", "failed": "destroying"},
	}
	statuses := []string{"provisioning", "active", "updating", "destroying", "failed", "destroyed"}

	for _, op := range []string{"PATCH", "DELETE"} {
		for _, from := range statuses {
			op, from := op, from
			t.Run(op+"_from_"+from, func(t *testing.T) {
				t.Parallel()
				before := tenantIn(t, from)
				tasksBefore := len(tasksOf(t, before.ID))

				var r Response
				if op == "PATCH" {
					r = patch(t, before.ID, before.Version, "matrix") // correct version: only status decides
				} else {
					r = del(t, before.ID)
				}

				after := getTenant(t, before.ID)
				tasksAfter := len(tasksOf(t, before.ID))

				if to, ok := allowed[op][from]; ok {
					expect(t, r, http.StatusAccepted, "")
					if after.Status != to || after.Version != before.Version+1 || tasksAfter != tasksBefore+1 {
						t.Fatalf("allowed: want %s v%d +1 task, got %s v%d tasks %d→%d",
							to, before.Version+1, after.Status, after.Version, tasksBefore, tasksAfter)
					}
					return
				}
				expect(t, r, http.StatusConflict, "tenant_update_not_allowed")
				if after.Status != before.Status || after.Version != before.Version || tasksAfter != tasksBefore {
					t.Fatalf("rejected call had side effects: %s v%d → %s v%d, tasks %d → %d",
						before.Status, before.Version, after.Status, after.Version, tasksBefore, tasksAfter)
				}
			})
		}
	}
}

// T2: every task status × every worker-reported status. Forward moves apply
// (including the accepted→done/failed jump); everything else is stale and
// must change nothing: task, tenant status and tenant version all unchanged.
func TestMatrix_TaskUpdates(t *testing.T) {
	requireNoWorker(t)

	type cell struct{ from, to string }
	applies := map[cell]bool{
		{"accepted", "in_progress"}: true,
		{"accepted", "done"}:        true, // forward jump
		{"accepted", "failed"}:      true, // forward jump
		{"in_progress", "done"}:     true,
		{"in_progress", "failed"}:   true,
	}
	tenantAfter := map[string]string{"in_progress": "provisioning", "done": "active", "failed": "failed"}

	for _, from := range []string{"accepted", "in_progress", "done", "failed"} {
		for _, to := range []string{"in_progress", "done", "failed"} {
			from, to := from, to
			t.Run(fmt.Sprintf("%s_then_%s", from, to), func(t *testing.T) {
				// Not parallel: barrier() relies on the consumer handling
				// task-updates in publish order, which holds per test.
				m := createTenant(t, slug("tm"))
				task, id := m.Task.ID, m.Tenant.ID

				// Drive the deploy task into `from`.
				if from != "accepted" {
					publishUpdate(t, task, from, uuid.Nil)
					waitTask(t, task, from)
				}
				before := getTenant(t, id)

				publishUpdate(t, task, to, uuid.Nil)

				if applies[cell{from, to}] {
					waitTask(t, task, to)
					after := waitTenant(t, id, tenantAfter[to])
					if to != "in_progress" && after.Version != before.Version+1 {
						t.Errorf("terminal outcome should bump version once: v%d → v%d", before.Version, after.Version)
					}
					return
				}

				// Stale: wait until the consumer is past it, then check nothing moved.
				barrier(t)
				if got := getTask(t, task).Status; got != from {
					t.Fatalf("stale update moved task %s → %s", from, got)
				}
				after := getTenant(t, id)
				if after.Status != before.Status || after.Version != before.Version {
					t.Fatalf("stale update changed tenant: %s v%d → %s v%d",
						before.Status, before.Version, after.Status, after.Version)
				}
			})
		}
	}
}
