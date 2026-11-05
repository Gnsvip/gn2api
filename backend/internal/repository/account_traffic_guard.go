package repository

import (
	"context"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.TrafficGuardRepository = (*accountRepository)(nil)

// Persist the cooldown and scheduler event in one statement. Concurrent
// completions cannot extend it or override an administrator/another breaker.
func (r *accountRepository) TrySetTrafficGuardCooldown(ctx context.Context, id int64, configJSON string, until time.Time, reason string) (bool, error) {
	result, err := r.sql.ExecContext(ctx, `
		WITH updated AS (
		 UPDATE accounts AS a
		 SET temp_unschedulable_until = $1, temp_unschedulable_reason = $2,
		 extra = COALESCE(a.extra, '{}'::jsonb) || jsonb_build_object('openai_traffic_guard_last', $2::jsonb), updated_at = NOW()
		 WHERE a.id = $3 AND a.deleted_at IS NULL
		 AND a.platform = 'openai' AND a.status = 'active' AND a.schedulable IS TRUE
		 AND a.extra -> 'openai_traffic_guard_v1' = $4::jsonb
		 AND a.extra -> 'openai_traffic_guard_v1' -> 'enabled' = 'true'::jsonb
		 AND (a.temp_unschedulable_until IS NULL OR a.temp_unschedulable_until <= NOW())
		 AND (a.rate_limit_reset_at IS NULL OR a.rate_limit_reset_at <= NOW())
		 AND (a.overload_until IS NULL OR a.overload_until <= NOW())
		 AND (a.auto_pause_on_expired IS NOT TRUE OR a.expires_at IS NULL OR a.expires_at > NOW())
		 RETURNING a.id
		)
		INSERT INTO scheduler_outbox (event_type, account_id, group_id, payload)
		SELECT $5, updated.id, NULL, NULL FROM updated
	`, until, reason, id, configJSON, service.SchedulerOutboxEventAccountChanged)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 0 {
		return false, err
	}
	r.syncSchedulerAccountSnapshotDetached(ctx, id)
	return true, nil
}
