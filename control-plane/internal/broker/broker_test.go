package broker

import "testing"

// Runs without RabbitMQ: checks the topology we will declare.
func TestTopology(t *testing.T) {
	specs := Topology()
	byName := map[string]queueSpec{}
	for i, q := range specs {
		byName[q.Name] = q
		// Every DLQ must be declared before the queue that routes to it.
		if dl, ok := q.Args["x-dead-letter-routing-key"].(string); ok {
			found := false
			for _, earlier := range specs[:i] {
				if earlier.Name == dl {
					found = true
				}
			}
			if !found {
				t.Errorf("%s dead-letters to %s, which is not declared before it", q.Name, dl)
			}
		}
	}

	for _, q := range []string{QueueTasks, QueueTaskUpdates} {
		main, ok := byName[q]
		if !ok {
			t.Fatalf("queue %s missing", q)
		}
		if main.Args["x-dead-letter-routing-key"] != DLQ(q) {
			t.Errorf("%s must dead-letter to %s, got %v", q, DLQ(q), main.Args)
		}
		if _, ok := byName[DLQ(q)]; !ok {
			t.Errorf("dead-letter queue %s missing", DLQ(q))
		}
	}
	if len(specs) != 4 {
		t.Errorf("expected 4 queues, got %d", len(specs))
	}
}