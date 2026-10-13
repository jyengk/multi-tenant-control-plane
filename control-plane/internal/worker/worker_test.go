package worker

import (
	"testing"
	"time"

	"controlplane/internal/domain"
)

func TestPlan_FailRate(t *testing.T) {
	cases := []struct {
		failRate, draw float64
		want           domain.TaskStatus
	}{
		{0, 0, domain.TaskDone},       // fail-rate 0 never fails
		{1, 0.999, domain.TaskFailed}, // fail-rate 1 always fails
		{0.3, 0.29, domain.TaskFailed},
		{0.3, 0.30, domain.TaskDone},
	}
	for _, c := range cases {
		w := New(Config{FailRate: c.failRate})
		w.rnd = func() float64 { return c.draw }
		got := w.plan()
		if len(got) != 2 || got[0] != domain.TaskInProgress || got[1] != c.want {
			t.Errorf("fail_rate=%v draw=%v: got %v, want [in_progress %s]", c.failRate, c.draw, got, c.want)
		}
	}
}

func TestRandomDelay_InRange(t *testing.T) {
	minD, maxD := 100*time.Millisecond, 300*time.Millisecond
	for i := 0; i < 1000; i++ {
		if d := randomDelay(minD, maxD); d < minD || d > maxD {
			t.Fatalf("delay %v outside [%v, %v]", d, minD, maxD)
		}
	}
	if d := randomDelay(minD, minD); d != minD {
		t.Fatalf("min==max should return min, got %v", d)
	}
}