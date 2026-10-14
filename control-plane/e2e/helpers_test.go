//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ---- Configuration -----------------------------------------------------------

var (
	apiURL  = env("E2E_API_URL", "http://localhost:8080")
	amqpURL = env("E2E_AMQP_URL", "amqp://controlplane:controlplane@localhost:5672/")

	httpClient = &http.Client{Timeout: 10 * time.Second}
)

const (
	queueTasks       = "tasks"
	queueTaskUpdates = "task-updates"
	queueUpdatesDLQ  = "task-updates.dlq"

	waitTimeout = 15 * time.Second // max wait for an async state change
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ---- API types (mirror the public JSON, nothing internal) -------------------

type Tenant struct {
	ID        string `json:"id"`
	Slug      string `json:"slug"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Version   int64  `json:"version"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type Task struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	TenantID string `json:"tenant_id"`
	Status   string `json:"status"`
}

type Mutation struct {
	Tenant Tenant `json:"tenant"`
	Task   Task   `json:"task"`
}

type Page[T any] struct {
	Data       []T     `json:"data"`
	NextCursor *string `json:"next_cursor"`
}

type APIError struct {
	Error struct {
		Code    string            `json:"code"`
		Message string            `json:"message"`
		Details map[string]string `json:"details"`
	} `json:"error"`
}

// Response is a raw API response.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// Code returns the error code of an error response ("" if not an error body).
func (r Response) Code() string {
	var e APIError
	_ = json.Unmarshal(r.Body, &e)
	return e.Error.Code
}

func (r Response) Decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, v); err != nil {
		t.Fatalf("decode %T: %v\nbody: %s", v, err, r.Body)
	}
}

// ---- HTTP helpers -------------------------------------------------------------

// call sends a request. body may be nil, a string (sent raw), or any value
// (sent as JSON).
//
// It reports transport failures with t.Errorf and returns Status 0, never
// t.Fatal: the concurrency tests call it from goroutines, where Fatal isn't
// allowed. The caller's expect() then fails the test on the main goroutine.
func call(t *testing.T, method, path string, body any) Response {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		buf, err := json.Marshal(b)
		if err != nil {
			t.Errorf("encode body: %v", err)
			return Response{}
		}
		rd = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, apiURL+path, rd)
	if err != nil {
		t.Errorf("build request: %v", err)
		return Response{}
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Errorf("%s %s: %v (is the stack up? docker compose up -d)", method, path, err)
		return Response{}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return Response{Status: resp.StatusCode, Header: resp.Header, Body: data}
}

// expect asserts status and, if code != "", the error code.
func expect(t *testing.T, r Response, status int, code string) {
	t.Helper()
	if r.Status != status {
		t.Fatalf("status: got %d, want %d\nbody: %s", r.Status, status, r.Body)
	}
	if code != "" && r.Code() != code {
		t.Fatalf("error code: got %q, want %q\nbody: %s", r.Code(), code, r.Body)
	}
}

func createTenant(t *testing.T, slug string) Mutation {
	t.Helper()
	r := call(t, "POST", "/v1/tenants", map[string]string{"slug": slug, "name": "E2E " + slug})
	expect(t, r, http.StatusAccepted, "")
	var m Mutation
	r.Decode(t, &m)
	return m
}

func getTenant(t *testing.T, id string) Tenant {
	t.Helper()
	r := call(t, "GET", "/v1/tenants/"+id, nil)
	expect(t, r, http.StatusOK, "")
	var tn Tenant
	r.Decode(t, &tn)
	return tn
}

func getTask(t *testing.T, id string) Task {
	t.Helper()
	r := call(t, "GET", "/v1/tasks/"+id, nil)
	expect(t, r, http.StatusOK, "")
	var tk Task
	r.Decode(t, &tk)
	return tk
}

func tasksOf(t *testing.T, tenantID string) []Task {
	t.Helper()
	r := call(t, "GET", "/v1/tasks?limit=100&tenant_id="+tenantID, nil)
	expect(t, r, http.StatusOK, "")
	var p Page[Task]
	r.Decode(t, &p)
	return p.Data
}

func patch(t *testing.T, id string, version int64, name string) Response {
	t.Helper()
	return call(t, "PATCH", "/v1/tenants/"+id, map[string]any{"version": version, "name": name})
}

func del(t *testing.T, id string) Response {
	t.Helper()
	return call(t, "DELETE", "/v1/tenants/"+id, nil)
}

// slug returns a unique, valid slug, so tests never collide with each other,
// with earlier runs, or with your manual data.
func slug(prefix string) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return prefix + "-" + string(b) // prefix ≤ 19 chars keeps it within 28
}

// eventually polls cond until it returns true or waitTimeout passes. On
// timeout it fails with the last state cond described.
func eventually(t *testing.T, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	var last string
	for time.Now().Before(deadline) {
		ok, state := cond()
		if ok {
			return
		}
		last = state
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s; last state: %s", waitTimeout, what, last)
}

// waitTenant waits until the tenant reaches status, and returns it.
func waitTenant(t *testing.T, id, status string) Tenant {
	t.Helper()
	var tn Tenant
	eventually(t, "tenant "+id+" → "+status, func() (bool, string) {
		tn = getTenant(t, id)
		return tn.Status == status, fmt.Sprintf("%s v%d", tn.Status, tn.Version)
	})
	return tn
}

// waitTask waits until the task reaches status.
func waitTask(t *testing.T, id, status string) {
	t.Helper()
	eventually(t, "task "+id+" → "+status, func() (bool, string) {
		tk := getTask(t, id)
		return tk.Status == status, tk.Status
	})
}

// ---- Broker helpers (the test playing the worker) -----------------------------

func amqpChannel(t *testing.T) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("connect to RabbitMQ at %s: %v", amqpURL, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

// publishRaw puts body on the task-updates queue exactly as given.
func publishRaw(t *testing.T, body []byte) {
	t.Helper()
	ch := amqpChannel(t)
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queueTaskUpdates, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := conf.WaitContext(ctx); err != nil || !ok {
		t.Fatalf("broker did not confirm publish: ok=%v err=%v", ok, err)
	}
}

// publishUpdate sends a worker-style progress update. Pass a fixed updateID
// to simulate a redelivery; uuid.Nil generates a fresh one.
func publishUpdate(t *testing.T, taskID, status string, updateID uuid.UUID) uuid.UUID {
	t.Helper()
	if updateID == uuid.Nil {
		updateID = uuid.Must(uuid.NewV7())
	}
	body, _ := json.Marshal(map[string]string{
		"update_id":   updateID.String(),
		"task_id":     taskID,
		"status":      status,
		"occurred_at": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	})
	publishRaw(t, body)
	return updateID
}

// inspect returns a queue's ready-message and consumer counts.
func inspect(t *testing.T, queue string) (messages, consumers int) {
	t.Helper()
	q, err := amqpChannel(t).QueueDeclarePassive(queue, true, false, false, false, amqp.Table{})
	if err != nil {
		t.Fatalf("inspect queue %s: %v", queue, err)
	}
	return q.Messages, q.Consumers
}

// barrier proves the consumer has processed everything published before it:
// it creates a fresh tenant, publishes its "done", and waits for it to become
// active. The consumer handles task-updates in order, so anything published
// earlier has been handled by then. Used to assert that a stale update
// changed nothing (there's no other observable signal for "ignored").
func barrier(t *testing.T) {
	t.Helper()
	m := createTenant(t, slug("barrier"))
	publishUpdate(t, m.Task.ID, "done", uuid.Nil)
	waitTenant(t, m.Tenant.ID, "active")
}

// ---- Worker mode -------------------------------------------------------------

// The worker is the only consumer of the "tasks" queue, so its consumer count
// tells us whether it is running.

func requireWorker(t *testing.T) {
	t.Helper()
	if _, c := inspect(t, queueTasks); c == 0 {
		t.Skip("needs the worker running: docker compose start worker")
	}
}

func requireNoWorker(t *testing.T) {
	t.Helper()
	if _, c := inspect(t, queueTasks); c > 0 {
		t.Skip("needs the worker STOPPED (the test plays the worker): docker compose stop worker")
	}
}

// ---- Driving tenants into a status (worker stopped) ----------------------------

// tenantIn creates a tenant and drives it into status by playing the worker.
// It returns the tenant as it is now (current version included).
func tenantIn(t *testing.T, status string) Tenant {
	t.Helper()
	m := createTenant(t, slug("st-"+status[:4]))
	id := m.Tenant.ID

	switch status {
	case "provisioning":
		return m.Tenant
	case "failed":
		publishUpdate(t, m.Task.ID, "failed", uuid.Nil)
		return waitTenant(t, id, "failed")
	}

	publishUpdate(t, m.Task.ID, "done", uuid.Nil)
	active := waitTenant(t, id, "active")

	switch status {
	case "active":
		return active
	case "updating":
		r := patch(t, id, active.Version, "renamed")
		expect(t, r, http.StatusAccepted, "")
		return getTenant(t, id)
	case "destroying":
		expect(t, del(t, id), http.StatusAccepted, "")
		return getTenant(t, id)
	case "destroyed":
		r := del(t, id)
		expect(t, r, http.StatusAccepted, "")
		var dm Mutation
		r.Decode(t, &dm)
		publishUpdate(t, dm.Task.ID, "done", uuid.Nil)
		return waitTenant(t, id, "destroyed")
	}
	t.Fatalf("tenantIn: unknown status %q", status)
	return Tenant{}
}
