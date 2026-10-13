package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The worker depends on this wire format; changing a JSON name is a breaking
// change, so pin it here.
func TestTaskMessageJSON(t *testing.T) {
	m := TaskMessage{
		ID:        uuid.MustParse("0192f3a1-7c2f-7000-8000-000000000001"),
		Type:      "deploy",
		TenantID:  uuid.MustParse("0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44"),
		Status:    "accepted",
		CreatedAt: time.Date(2026, 10, 13, 17, 43, 24, 123_000_000, time.UTC),
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"0192f3a1-7c2f-7000-8000-000000000001","type":"deploy",` +
		`"tenant_id":"0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44","status":"accepted",` +
		`"created_at":"2026-10-13T17:43:24.123Z"}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
}