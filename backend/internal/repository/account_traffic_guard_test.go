package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTrafficGuardCooldownConditionalAtomicPersistence(t *testing.T) {
	for _, affected := range []int64{0, 1} {
		exec := &recordingSQLExecutor{result: rowsAffectedResult(affected)}
		repo := newAccountRepositoryWithSQL(nil, exec, nil)
		applied, err := repo.TrySetTrafficGuardCooldown(context.Background(), 7, `{"enabled":true}`, time.Now().Add(time.Minute), `{"traffic_guard":{}}`)
		require.NoError(t, err)
		require.Equal(t, affected == 1, applied)
		require.Len(t, exec.execQueries, 1)
		q := normalizeSQLWhitespace(exec.execQueries[0])
		for _, clause := range []string{"WITH updated AS", "INSERT INTO scheduler_outbox", "FROM updated", "a.deleted_at IS NULL", "a.schedulable IS TRUE", "a.status = 'active'", "a.extra -> 'openai_traffic_guard_v1' = $4::jsonb", "a.temp_unschedulable_until <= NOW()", "a.rate_limit_reset_at <= NOW()", "a.overload_until <= NOW()", "jsonb_build_object('openai_traffic_guard_last'"} {
			require.Contains(t, q, clause)
		}
		require.Len(t, exec.execArgs[0], 5)
	}
}
