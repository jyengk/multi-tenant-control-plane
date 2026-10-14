//go:build e2e

package e2e

import (
	"net/http"
	"sync"
	"testing"
)

// Race conditions over real HTTP: many simultaneous requests, exactly one
// winner. Worker STOPPED, so tenants stay in the state the test puts them in.

// fire runs n copies of fn at the same moment and returns their statuses.
func fire(n int, fn func() Response) []int {
	var wg sync.WaitGroup
	start := make(chan struct{})
	statuses := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // release all goroutines together
			statuses[i] = fn().Status
		}(i)
	}
	close(start)
	wg.Wait()
	return statuses
}

func count(statuses []int, code int) int {
	n := 0
	for _, s := range statuses {
		if s == code {
			n++
		}
	}
	return n
}

func TestConcurrency_SameSlugCreates(t *testing.T) {
	requireNoWorker(t)
	s := slug("race")
	statuses := fire(20, func() Response {
		return call(t, "POST", "/v1/tenants", map[string]string{"slug": s, "name": "race"})
	})
	if w, l := count(statuses, http.StatusAccepted), count(statuses, http.StatusConflict); w != 1 || l != 19 {
		t.Fatalf("want 1×202 + 19×409, got %d×202 + %d×409 (%v)", w, l, statuses)
	}
	t.Logf("slug=%s: 20 concurrent POSTs → 1×202, 19×409", s)
}

// The brief's non-functional requirement #1.
func TestConcurrency_SameVersionPatches(t *testing.T) {
	requireNoWorker(t)
	tn := tenantIn(t, "active")
	statuses := fire(20, func() Response { return patch(t, tn.ID, tn.Version, "racer") })

	if w, l := count(statuses, http.StatusAccepted), count(statuses, http.StatusConflict); w != 1 || l != 19 {
		t.Fatalf("want 1×202 + 19×409, got %d×202 + %d×409 (%v)", w, l, statuses)
	}
	after := getTenant(t, tn.ID)
	if after.Version != tn.Version+1 {
		t.Errorf("version bumped %d times", after.Version-tn.Version)
	}
	if n := len(tasksOf(t, tn.ID)); n != 2 { // deploy + exactly one update
		t.Errorf("want 2 tasks, got %d", n)
	}
	t.Logf("slug=%s id=%s: 20 concurrent PATCHes at v%d → 1×202, 19×409; now v%d",
		tn.Slug, tn.ID, tn.Version, after.Version)
}

func TestConcurrency_Deletes(t *testing.T) {
	requireNoWorker(t)
	tn := tenantIn(t, "active")
	statuses := fire(10, func() Response { return del(t, tn.ID) })
	if w := count(statuses, http.StatusAccepted); w != 1 {
		t.Fatalf("want exactly 1×202, got %d (%v)", w, statuses)
	}
	t.Logf("slug=%s id=%s: 10 concurrent DELETEs → 1×202", tn.Slug, tn.ID)
}

// PATCH and DELETE racing on one active tenant: exactly one wins, and the
// tenant ends in the winner's state with exactly one new task.
func TestConcurrency_PatchVersusDelete(t *testing.T) {
	requireNoWorker(t)
	for i := 0; i < 10; i++ {
		tn := tenantIn(t, "active")
		var patchStatus, deleteStatus int
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() { defer wg.Done(); <-start; patchStatus = patch(t, tn.ID, tn.Version, "p").Status }()
		go func() { defer wg.Done(); <-start; deleteStatus = del(t, tn.ID).Status }()
		close(start)
		wg.Wait()

		wins := count([]int{patchStatus, deleteStatus}, http.StatusAccepted)
		if wins != 1 {
			t.Fatalf("round %d: want exactly one winner, PATCH=%d DELETE=%d", i, patchStatus, deleteStatus)
		}
		after := getTenant(t, tn.ID)
		want := "updating"
		if deleteStatus == http.StatusAccepted {
			want = "destroying"
		}
		if after.Status != want || after.Version != tn.Version+1 {
			t.Fatalf("round %d: want %s v%d, got %s v%d", i, want, tn.Version+1, after.Status, after.Version)
		}
		t.Logf("round %d slug=%s id=%s: PATCH=%d DELETE=%d → %s v%d",
			i, tn.Slug, tn.ID, patchStatus, deleteStatus, after.Status, after.Version)
	}
}