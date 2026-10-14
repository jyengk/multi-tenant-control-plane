//go:build e2e

package e2e

import (
	"net/http"
	"testing"
)

// Happy paths, driven by the REAL worker (fail-rate 0).

func TestLifecycle_DeployUpdateDestroy(t *testing.T) {
	requireWorker(t)

	// Create → provisioning v1, deploy task accepted.
	r := call(t, "POST", "/v1/tenants", map[string]string{"slug": slug("life"), "name": "Lifecycle"})
	expect(t, r, http.StatusAccepted, "")
	var m Mutation
	r.Decode(t, &m)
	if m.Tenant.Status != "provisioning" || m.Tenant.Version != 1 {
		t.Fatalf("created tenant: %+v", m.Tenant)
	}
	if m.Task.Type != "deploy" || m.Task.Status != "accepted" || m.Task.TenantID != m.Tenant.ID {
		t.Fatalf("deploy task: %+v", m.Task)
	}
	if loc := r.Header.Get("Location"); loc != "/v1/tenants/"+m.Tenant.ID {
		t.Errorf("Location header: %q", loc)
	}
	id := m.Tenant.ID

	// Worker deploys → active v2, task done.
	tn := waitTenant(t, id, "active")
	if tn.Version != 2 {
		t.Errorf("after deploy: want v2, got v%d", tn.Version)
	}
	waitTask(t, m.Task.ID, "done")

	// PATCH → updating v3; worker → active v4 with the new name.
	r = patch(t, id, tn.Version, "Lifecycle Renamed")
	expect(t, r, http.StatusAccepted, "")
	var pm Mutation
	r.Decode(t, &pm)
	if pm.Tenant.Status != "updating" || pm.Tenant.Version != 3 || pm.Task.Type != "update" {
		t.Fatalf("after PATCH: %+v %+v", pm.Tenant, pm.Task)
	}
	tn = waitTenant(t, id, "active")
	if tn.Version != 4 || tn.Name != "Lifecycle Renamed" {
		t.Errorf("after update: want active v4 'Lifecycle Renamed', got %s v%d %q", tn.Status, tn.Version, tn.Name)
	}

	// DELETE → destroying; worker → destroyed. Row stays readable.
	r = del(t, id)
	expect(t, r, http.StatusAccepted, "")
	var dm Mutation
	r.Decode(t, &dm)
	if dm.Tenant.Status != "destroying" || dm.Task.Type != "destroy" {
		t.Fatalf("after DELETE: %+v %+v", dm.Tenant, dm.Task)
	}
	tn = waitTenant(t, id, "destroyed")
	if tn.Version != 6 {
		t.Errorf("after destroy: want v6, got v%d", tn.Version)
	}

	// Task history: deploy, update, destroy, in that order, all done.
	tasks := tasksOf(t, id)
	want := []string{"deploy", "update", "destroy"}
	if len(tasks) != len(want) {
		t.Fatalf("want %d tasks, got %d: %+v", len(want), len(tasks), tasks)
	}
	for i, tk := range tasks {
		if tk.Type != want[i] || tk.Status != "done" {
			t.Errorf("task %d: want %s/done, got %s/%s", i, want[i], tk.Type, tk.Status)
		}
	}
}

func TestLifecycle_ListFiltersAndPagination(t *testing.T) {
	requireWorker(t)

	ids := map[string]bool{}
	for i := 0; i < 3; i++ {
		m := createTenant(t, slug("page"))
		ids[m.Tenant.ID] = true
		waitTenant(t, m.Tenant.ID, "active")
	}

	// Walk every page with limit=2: each of our tenants appears exactly once,
	// and IDs are strictly increasing (creation order, stable cursor).
	seen := map[string]int{}
	cursor, prev, pages := "", "", 0
	for {
		path := "/v1/tenants?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		r := call(t, "GET", path, nil)
		expect(t, r, http.StatusOK, "")
		var p Page[Tenant]
		r.Decode(t, &p)
		if len(p.Data) > 2 {
			t.Fatalf("page larger than limit: %d", len(p.Data))
		}
		for _, tn := range p.Data {
			seen[tn.ID]++
			if tn.ID <= prev {
				t.Fatalf("not in creation order: %s after %s", tn.ID, prev)
			}
			prev = tn.ID
		}
		pages++
		if p.NextCursor == nil {
			break
		}
		cursor = *p.NextCursor
		if pages > 10_000 {
			t.Fatal("pagination did not terminate")
		}
	}
	for id := range ids {
		if seen[id] != 1 {
			t.Errorf("tenant %s seen %d times across pages", id, seen[id])
		}
	}

	// Status filter.
	var id string
	for k := range ids {
		id = k
		break
	}
	r := call(t, "GET", "/v1/tenants?limit=100&status=destroyed", nil)
	expect(t, r, http.StatusOK, "")
	var p Page[Tenant]
	r.Decode(t, &p)
	for _, tn := range p.Data {
		if tn.Status != "destroyed" {
			t.Errorf("status filter returned %s", tn.Status)
		}
		if tn.ID == id {
			t.Errorf("active tenant returned by status=destroyed")
		}
	}

	// Task filters.
	r = call(t, "GET", "/v1/tasks?type=deploy&status=done&tenant_id="+id, nil)
	expect(t, r, http.StatusOK, "")
	var tp Page[Task]
	r.Decode(t, &tp)
	if len(tp.Data) != 1 || tp.Data[0].Type != "deploy" {
		t.Errorf("task filter: %+v", tp.Data)
	}
}

func TestLifecycle_Health(t *testing.T) {
	expect(t, call(t, "GET", "/healthz", nil), http.StatusOK, "")
	expect(t, call(t, "GET", "/readyz", nil), http.StatusOK, "")
}
