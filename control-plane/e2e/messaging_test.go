//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// Messaging behaviour, with the test playing the worker (worker STOPPED).

func TestMessaging_FailedOutcome(t *testing.T) {
	requireNoWorker(t)
	m := createTenant(t, slug("fail"))
	publishUpdate(t, m.Task.ID, "in_progress", uuid.Nil)
	publishUpdate(t, m.Task.ID, "failed", uuid.Nil)

	tn := waitTenant(t, m.Tenant.ID, "failed")
	waitTask(t, m.Task.ID, "failed")
	if tn.Version != 2 {
		t.Errorf("want v2, got v%d", tn.Version)
	}
}

// The same update_id delivered three times is applied once.
func TestMessaging_DuplicateIsIdempotent(t *testing.T) {
	requireNoWorker(t)
	m := createTenant(t, slug("dupe"))
	id := publishUpdate(t, m.Task.ID, "done", uuid.Nil)
	publishUpdate(t, m.Task.ID, "done", id)
	publishUpdate(t, m.Task.ID, "done", id)

	waitTenant(t, m.Tenant.ID, "active")
	barrier(t)
	if tn := getTenant(t, m.Tenant.ID); tn.Status != "active" || tn.Version != 2 {
		t.Fatalf("duplicates changed state: %s v%d (want active v2)", tn.Status, tn.Version)
	}
}

// Out-of-order: in_progress arriving after done must not regress anything.
func TestMessaging_OutOfOrder(t *testing.T) {
	requireNoWorker(t)
	m := createTenant(t, slug("ooo"))
	publishUpdate(t, m.Task.ID, "done", uuid.Nil)
	waitTenant(t, m.Tenant.ID, "active")

	publishUpdate(t, m.Task.ID, "in_progress", uuid.Nil)
	barrier(t)

	if s := getTask(t, m.Task.ID).Status; s != "done" {
		t.Fatalf("task regressed to %s", s)
	}
	if tn := getTenant(t, m.Tenant.ID); tn.Status != "active" || tn.Version != 2 {
		t.Fatalf("tenant changed: %s v%d", tn.Status, tn.Version)
	}
}

// Poison messages go to the DLQ, and the consumer keeps working afterwards.
func TestMessaging_PoisonMessagesAreDeadLettered(t *testing.T) {
	requireNoWorker(t)
	before, _ := inspect(t, queueUpdatesDLQ)

	poison := [][]byte{
		[]byte(`not json at all`),
		[]byte(`{"task_id":"0192f3a1-7c2f-7000-8000-000000000001","status":"done"}`),                                                        // no update_id
		[]byte(`{"update_id":"0192f3a2-0000-7000-8000-00000000abcd","status":"done"}`),                                                      // no task_id
		[]byte(`{"update_id":"0192f3a2-0000-7000-8000-00000000abce","task_id":"0192f3a1-7c2f-7000-8000-000000000001","status":"exploded"}`), // bad status
		[]byte(`{"update_id":"0192f3a2-0000-7000-8000-00000000abcf","task_id":"0192f3a1-7c2f-7000-8000-000000000001","status":"accepted"}`), // not a worker status
	}
	for _, p := range poison {
		publishRaw(t, p)
	}
	// Unknown task: well-formed, but nothing to apply it to.
	publishUpdate(t, uuid.NewString(), "done", uuid.Nil)
	want := before + len(poison) + 1

	// The consumer is still alive: a valid update after the poison still applies.
	barrier(t)

	// DLQ count can lag the ack by a moment.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, _ := inspect(t, queueUpdatesDLQ)
		if got >= want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task-updates.dlq: want ≥ %d messages, got %d", want, got)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Many tenants at once all reach their correct final state.
func TestMessaging_ManyTenantsInParallel(t *testing.T) {
	requireNoWorker(t)
	const n = 20
	type pair struct{ tenant, task, want string }
	pairs := make([]pair, n)
	for i := range pairs {
		m := createTenant(t, slug("many"))
		outcome, want := "done", "active"
		if i%3 == 0 {
			outcome, want = "failed", "failed"
		}
		publishUpdate(t, m.Task.ID, "in_progress", uuid.Nil)
		publishUpdate(t, m.Task.ID, outcome, uuid.Nil)
		pairs[i] = pair{m.Tenant.ID, m.Task.ID, want}
	}
	for _, p := range pairs {
		waitTenant(t, p.tenant, p.want)
	}
}
