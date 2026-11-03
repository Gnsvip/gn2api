//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type observingBackupDumper struct {
	*mockDumper
	dump func(context.Context) (io.ReadCloser, error)
}

func (d *observingBackupDumper) Dump(ctx context.Context) (io.ReadCloser, error) {
	return d.dump(ctx)
}

func TestScheduledBackupVisibleBeforeDumpStarts(t *testing.T) {
	repo := newMockSettingRepo()
	seedS3Config(t, repo)
	var svc *BackupService
	dumper := &observingBackupDumper{mockDumper: &mockDumper{}, dump: func(ctx context.Context) (io.ReadCloser, error) {
		records, err := svc.ListBackups(ctx)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, "scheduled", records[0].TriggeredBy)
		require.Equal(t, "running", records[0].Status)
		require.Equal(t, "dumping", records[0].Progress)
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Positive(t, time.Until(deadline))
		require.LessOrEqual(t, time.Until(deadline), backupOperationTimeout)
		return io.NopCloser(strings.NewReader("synthetic backup")), nil
	}}
	svc = newTestBackupService(repo, dumper, newMockObjectStore())
	svc.runScheduledBackup()
	records, err := svc.ListBackups(context.Background())
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "completed", records[0].Status)
	require.Empty(t, records[0].Progress)
}

type rejectingBackupRecordRepo struct{ *mockSettingRepo }

func (r *rejectingBackupRecordRepo) Set(ctx context.Context, key, value string) error {
	if key == settingKeyBackupRecords {
		return errors.New("injected record persistence failure")
	}
	return r.mockSettingRepo.Set(ctx, key, value)
}

func TestBackupDoesNotDumpWithoutRunningRecord(t *testing.T) {
	repo := newMockSettingRepo()
	seedS3Config(t, repo)
	called := false
	dumper := &observingBackupDumper{mockDumper: &mockDumper{}, dump: func(context.Context) (io.ReadCloser, error) {
		called = true
		return io.NopCloser(strings.NewReader("synthetic")), nil
	}}
	svc := newTestBackupService(repo, dumper, newMockObjectStore())
	svc.settingRepo = &rejectingBackupRecordRepo{repo}
	_, err := svc.CreateBackup(context.Background(), "scheduled", 14)
	require.ErrorContains(t, err, "save initial record")
	require.False(t, called)
}

func TestBackupCancellationPersistsTerminalRecord(t *testing.T) {
	repo := newMockSettingRepo()
	seedS3Config(t, repo)
	ctx, cancel := context.WithCancel(context.Background())
	dumper := &observingBackupDumper{mockDumper: &mockDumper{}, dump: func(ctx context.Context) (io.ReadCloser, error) {
		cancel()
		return nil, ctx.Err()
	}}
	svc := newTestBackupService(repo, dumper, newMockObjectStore())
	record, err := svc.CreateBackup(ctx, "scheduled", 14)
	require.ErrorIs(t, err, context.Canceled)
	stored, err := svc.GetBackupRecord(context.Background(), record.ID)
	require.NoError(t, err)
	require.Equal(t, "failed", stored.Status)
	require.Empty(t, stored.Progress)
	require.NotEmpty(t, stored.FinishedAt)
}

func TestBackupShutdownTimeoutCancelsAndJoinsActiveDump(t *testing.T) {
	repo := newMockSettingRepo()
	seedS3Config(t, repo)
	started := make(chan struct{})
	dumper := &observingBackupDumper{mockDumper: &mockDumper{}, dump: func(ctx context.Context) (io.ReadCloser, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	svc := newTestBackupService(repo, dumper, newMockObjectStore())
	record, err := svc.StartBackup(context.Background(), "manual", 14)
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("dump did not start")
	}
	svc.stopWithTimeout(10 * time.Millisecond)
	stored, err := svc.GetBackupRecord(context.Background(), record.ID)
	require.NoError(t, err)
	require.Equal(t, "failed", stored.Status)
	require.Empty(t, stored.Progress)
	require.ErrorIs(t, svc.bgCtx.Err(), context.Canceled)
	_, err = svc.StartBackup(context.Background(), "manual", 14)
	require.ErrorContains(t, err, "shutting down")
}
