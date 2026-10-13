package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"controlplane/internal/domain"
)

// UpdateOutcome says what applying a worker update did.
type UpdateOutcome string

// Outcomes. Applied and StaleIgnored are recorded in processed_messages;
// Duplicate means the update_id was already recorded, so nothing was written.
const (
	OutcomeApplied      UpdateOutcome = "applied"
	OutcomeStaleIgnored UpdateOutcome = "stale_ignored"
	OutcomeDuplicate    UpdateOutcome = "duplicate"
)

// ErrUnknownTask means the update refers to a task that doesn't exist. Retrying
// can't fix that, so the consumer dead-letters the message.
var ErrUnknownTask = errors.New("update refers to an unknown task")

// TaskUpdate is a validated worker progress update.
type TaskUpdate struct {
	UpdateID uuid.UUID
	TaskID   uuid.UUID
	Status   domain.TaskStatus // in_progress, done or failed
}

// ApplyTaskUpdate applies one worker update in a single transaction:
//
//  1. lock the task row (serialises concurrent updates for the same task)
//  2. if update_id was already processed → duplicate: change nothing
//  3. move the task forward if allowed, else ignore it as stale
//  4. on a terminal outcome, move the tenant and bump its version
//  5. record update_id in processed_messages
//
// Idempotent: a redelivered update stops at step 2. Order-tolerant: an
// update that would move the task backwards (in_progress after done, or
// anything after a terminal state) is recorded as stale_ignored and changes
// nothing.
func (s *Store) ApplyTaskUpdate(ctx context.Context, u TaskUpdate) (UpdateOutcome, error) {
	var outcome UpdateOutcome
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// 1. Lock the task. FOR UPDATE makes a second update for this task
		// wait until we commit, so it sees our processed_messages row and
		// our new status, rather than racing us.
		var typ, current string
		var tenantID uuid.UUID
		err := tx.QueryRow(ctx,
			`SELECT type, tenant_id, status FROM tasks WHERE id = $1 FOR UPDATE`, u.TaskID,
		).Scan(&typ, &tenantID, &current)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrUnknownTask
		}
		if err != nil {
			return fmt.Errorf("lock task: %w", err)
		}

		// 2. Idempotency check.
		var seen bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM processed_messages WHERE update_id = $1)`, u.UpdateID,
		).Scan(&seen); err != nil {
			return fmt.Errorf("check processed: %w", err)
		}
		if seen {
			outcome = OutcomeDuplicate
			return nil // nothing to write; commit is a no-op
		}

		// 3. Forward-only task transition (rules from domain).
		outcome = OutcomeStaleIgnored
		if domain.CanTransitionTask(domain.TaskStatus(current), u.Status) {
			outcome = OutcomeApplied
			if _, err := tx.Exec(ctx,
				`UPDATE tasks SET status = $2, updated_at = now() WHERE id = $1`,
				u.TaskID, string(u.Status)); err != nil {
				return fmt.Errorf("update task: %w", err)
			}

			// 4. Terminal outcome → tenant moves (deploy done → active, any
			// failure → failed, destroy done → destroyed). The WHERE status
			// guard is defensive: the API guards make a mismatch impossible,
			// but if one ever happens we leave the tenant alone and log it.
			if tr, terminal := domain.TenantOnOutcome(domain.TaskType(typ), u.Status); terminal {
				tag, err := tx.Exec(ctx, `
					UPDATE tenants
					   SET status = $2, version = version + 1, updated_at = now()
					 WHERE id = $1 AND status = $3`,
					tenantID, string(tr.To), string(tr.From))
				if err != nil {
					return fmt.Errorf("update tenant: %w", err)
				}
				if tag.RowsAffected() == 0 {
					slog.Warn("task outcome did not match tenant status; tenant left unchanged",
						"tenant_id", tenantID, "task_id", u.TaskID, "expected_status", tr.From)
				}
			}
		}

		// 5. Record the update (applied or stale) so a redelivery is a no-op.
		if _, err := tx.Exec(ctx, `
			INSERT INTO processed_messages (update_id, task_id, status, outcome, processed_at)
			VALUES ($1, $2, $3, $4, now())`,
			u.UpdateID, u.TaskID, string(u.Status), string(outcome)); err != nil {
			if pgCode(err) == sqlUniqueViolation {
				// Same update committed by another consumer between our
				// check and insert: it's a duplicate after all.
				outcome = OutcomeDuplicate
				return errDuplicateRace
			}
			return fmt.Errorf("record processed: %w", err)
		}
		return nil
	})
	if errors.Is(err, errDuplicateRace) {
		return OutcomeDuplicate, nil // our transaction rolled back; the other one won
	}
	return outcome, err
}

// errDuplicateRace forces a rollback when another consumer recorded the same
// update first. Internal only: callers see OutcomeDuplicate.
var errDuplicateRace = errors.New("duplicate update recorded concurrently")