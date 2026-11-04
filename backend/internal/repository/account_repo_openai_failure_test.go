package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAccountRepository_OpenAIFailureCAS(t *testing.T) {
	for _, affected := range []int64{0, 1} {
		exec := &recordingSQLExecutor{result: rowsAffectedResult(affected)}
		repo := newAccountRepositoryWithSQL(nil, exec, nil)
		applied, err := repo.SetOpenAIOAuthErrorIfCredentialsUnchanged(context.Background(), 42, map[string]any{"access_token": "observed", "_token_version": int64(7)}, "sign in again")
		require.NoError(t, err)
		require.Equal(t, affected == 1, applied)
		require.Len(t, exec.execQueries, 1)
		query := normalizeSQLWhitespace(exec.execQueries[0])
		for _, clause := range []string{"a.deleted_at IS NULL", "a.platform = $4", "a.type = $5", "a.status = $6", "a.credentials = $7::jsonb", "INSERT INTO scheduler_outbox", "FROM updated"} {
			require.Contains(t, query, clause)
		}
		require.Equal(t, service.PlatformOpenAI, exec.execArgs[0][3])
		require.Equal(t, service.AccountTypeOAuth, exec.execArgs[0][4])
		require.Equal(t, service.StatusActive, exec.execArgs[0][5])
		require.Contains(t, exec.execArgs[0][6], `"_token_version":7`)
	}
	exec := &recordingSQLExecutor{err: errors.New("outbox insert failed")}
	repo := newAccountRepositoryWithSQL(nil, exec, nil)
	applied, err := repo.SetOpenAIOAuthErrorIfCredentialsUnchanged(context.Background(), 42, nil, "rejected")
	require.Error(t, err)
	require.False(t, applied)
	require.Len(t, exec.execQueries, 1, "outbox failure must roll back the state mutation")
}
