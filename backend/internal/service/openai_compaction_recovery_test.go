package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Restart recovery is a correctness/security boundary: keep these cases in the
// existing suite to guard against lost history and cross-session restoration.
func TestCompactionRecoveryProductionStartup(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DATA_DIR", dir)
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	svc.cfg.JWT.Secret = "production-startup-test-secret"
	svc.StartCompactionRecoveryPersistence()
	t.Cleanup(svc.CloseOpenAIWSPool)
	require.NotNil(t, svc.compactionRecoveryStore().disk)
	require.DirExists(t, filepath.Join(dir, "compaction-recovery"))
}

func TestCompactionRecoveryPersistence(t *testing.T) {
	newStore := func(t *testing.T, dir, key string) *openAICompactionRecoveryStore {
		t.Helper()
		store := (&OpenAIGatewayService{}).compactionRecoveryStore()
		require.NoError(t, store.startPersistence(dir, key))
		t.Cleanup(store.closePersistence)
		return store
	}
	source := recoveryItems(t, recoveryImage+","+autoContinuationCall+","+autoContinuationTool+","+recoveryRetained)
	output := recoveryItems(t, recoverySummary+","+recoveryRetained)
	body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
	body, _ = sjson.SetBytes(body, "previous_response_id", "resp_expired")
	for _, scenario := range []string{"restart", "rejected_before_restart", "corrupt", "wrong_key", "expired", "edited_window", "other_scope", "replacement"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			store := newStore(t, dir, "test-secret")
			require.True(t, store.put("scope", source, output))
			if scenario == "rejected_before_restart" {
				store.mu.Lock()
				store.entries["scope:"+openAIEncryptedContentDigest("recovery-test-cipher")].invalid = true
				store.notifyPersistence()
				store.mu.Unlock()
			}
			if scenario == "expired" {
				store.mu.Lock()
				for _, e := range store.entries {
					e.expires = time.Now().Add(300 * time.Millisecond)
				}
				store.mu.Unlock()
			}
			if scenario == "replacement" {
				fresh := recoveryItems(t, strings.ReplaceAll(recoverySummary, "recovery-test-cipher", "fresh-cipher"))
				require.True(t, store.putReplacement("scope", 42, body, source, fresh))
			}
			store.closePersistence() // Graceful restart drains asynchronous writes.
			files, err := os.ReadDir(dir)
			require.NoError(t, err)
			for _, f := range files {
				require.NotContains(t, f.Name(), ".tmp")
				data, err := os.ReadFile(filepath.Join(dir, f.Name()))
				require.NoError(t, err)
				require.NotContains(t, string(data), "7391")
				require.NotContains(t, string(data), "recovery-test-cipher")
				if scenario == "corrupt" {
					data[len(data)-1] ^= 1
					require.NoError(t, os.WriteFile(filepath.Join(dir, f.Name()), data, 0600))
				}
			}
			key := "test-secret"
			if scenario == "wrong_key" {
				key = "different-secret"
			}
			if scenario == "expired" {
				time.Sleep(350 * time.Millisecond)
			}
			loaded := newStore(t, dir, key)
			if scenario == "expired" || scenario == "corrupt" || scenario == "wrong_key" {
				require.Empty(t, loaded.entries)
				files, err = os.ReadDir(dir)
				require.NoError(t, err)
				require.Empty(t, files)
				return
			}
			nextBody, scope := body, "scope"
			if scenario == "edited_window" {
				nextBody = []byte(strings.ReplaceAll(string(body), "retained-tail", "edited-tail"))
			}
			if scenario == "other_scope" {
				scope = "other-key-thread-group"
			}
			next, ok := loaded.restore(scope, nextBody, scenario != "rejected_before_restart")
			if scenario == "edited_window" || scenario == "other_scope" {
				require.False(t, ok)
				require.Equal(t, nextBody, next)
				return
			}
			require.True(t, ok)
			require.False(t, gjson.GetBytes(next, "previous_response_id").Exists())
			require.Contains(t, string(next), "7391")
			require.Contains(t, string(next), "data:image/png;base64,AAA=")
			require.JSONEq(t, "["+recoveryImage+","+autoContinuationCall+","+autoContinuationTool+","+recoveryRetained+","+recoveryUser+"]", gjson.GetBytes(next, "input").Raw)
			require.Equal(t, "xhigh", gjson.GetBytes(next, "reasoning.effort").String())
			if scenario == "replacement" {
				replaced, ok := loaded.replace("scope", 42, next)
				require.True(t, ok)
				require.Contains(t, string(replaced), "fresh-cipher")
				_, ok = loaded.replace("scope", 43, next)
				require.False(t, ok, "replacement cipher belongs to its producing account")
			}
		})
	}
}

func TestCompactionRecoveryPersistencePrunesOnlyOwnedFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("untouched"), 0600))
	stale := compactionDiskName("old") + ".tmp"
	require.NoError(t, os.WriteFile(filepath.Join(dir, stale), []byte("partial"), 0600))
	store := (&OpenAIGatewayService{}).compactionRecoveryStore()
	require.NoError(t, store.startPersistence(dir, "test-secret"))
	t.Cleanup(store.closePersistence)
	source := recoveryItems(t, recoveryImage)
	for i := range openAICompactionRecoveryPerScope + 2 {
		require.True(t, store.put("scope", source, recoveryItems(t, strings.ReplaceAll(recoverySummary, "recovery-test-cipher", fmt.Sprint(i)))))
	}
	store.closePersistence()
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, openAICompactionRecoveryPerScope+1)
	_, err = os.Stat(filepath.Join(dir, stale))
	require.True(t, os.IsNotExist(err))
	keep, err := os.ReadFile(filepath.Join(dir, "keep.txt"))
	require.NoError(t, err)
	require.Equal(t, "untouched", string(keep))
}

func TestCompactionRecoveryPersistenceProcessRestart(t *testing.T) {
	if dir := os.Getenv("GN2API_RECOVERY_TEST_DIR"); dir != "" {
		store := (&OpenAIGatewayService{}).compactionRecoveryStore()
		require.NoError(t, store.startPersistence(dir, "process-test-secret"))
		defer store.closePersistence()
		if os.Getenv("GN2API_RECOVERY_TEST_PHASE") == "write" {
			require.True(t, store.put("scope", recoveryItems(t, recoveryImage+","+recoveryRetained), recoveryItems(t, recoverySummary+","+recoveryRetained)))
			return
		}
		body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
		next, ok := store.restore("scope", body, true)
		require.True(t, ok)
		require.JSONEq(t, "["+recoveryImage+","+recoveryRetained+","+recoveryUser+"]", gjson.GetBytes(next, "input").Raw)
		return
	}
	dir := t.TempDir()
	for _, phase := range []string{"write", "read"} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCompactionRecoveryPersistenceProcessRestart$", "-test.count=1")
		cmd.Env = append(os.Environ(), "GN2API_RECOVERY_TEST_DIR="+dir, "GN2API_RECOVERY_TEST_PHASE="+phase)
		out, err := cmd.CombinedOutput()
		cancel()
		require.NoError(t, err, "%s: %s", phase, out)
	}
}

func TestCompactionRecoveryPersistenceConcurrentExpiry(t *testing.T) {
	dir := t.TempDir()
	store := (&OpenAIGatewayService{}).compactionRecoveryStore()
	require.NoError(t, store.startPersistence(dir, "test-secret"))
	t.Cleanup(store.closePersistence)
	source, output := recoveryItems(t, recoveryImage), recoveryItems(t, recoverySummary)
	body := compactionTestBody(recoverySummary+","+recoveryUser, true)
	var wg sync.WaitGroup
	failures := make(chan string, 32)
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scope := fmt.Sprint(i)
			if !store.put(scope, source, output) {
				failures <- "put"
				return
			}
			for range 5 {
				if _, ok := store.restore(scope, body, true); !ok {
					failures <- "restore"
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	store.closePersistence()
	loaded := (&OpenAIGatewayService{}).compactionRecoveryStore()
	require.NoError(t, loaded.startPersistence(dir, "test-secret"))
	t.Cleanup(loaded.closePersistence)
	require.Len(t, loaded.entries, 32)
	loaded.mu.Lock()
	var expired *openAICompactionRecoveryEntry
	for _, e := range loaded.entries {
		expired = e
		break
	}
	expired.expires = time.Now().Add(-time.Second)
	loaded.mu.Unlock()
	loaded.expire(expired)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dir, compactionDiskName(expired.key)))
		return os.IsNotExist(err)
	}, 3*time.Second, 10*time.Millisecond, "idle expiry must also remove the disk snapshot")
	loaded.closePersistence()
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 31)
}

const recoveryImage = `{"type":"message","role":"user","content":[{"type":"input_text","text":"The secret before compression is 7391."},{"type":"input_image","image_url":"data:image/png;base64,AAA="}]}`
const recoveryRetained = `{"type":"message","role":"user","content":[{"type":"input_text","text":"retained-tail"}]}`
const recoverySummary = `{"type":"compaction","id":"cmp_test","encrypted_content":"recovery-test-cipher"}`
const recoveryUser = `{"type":"message","role":"user","content":[{"type":"input_text","text":"Continue using the earlier secret and picture."}]}`
const recoveryTrigger = `{"type":"compaction_trigger"}`
const recoveryCipherError = `{"type":"response.failed","response":{"id":"resp_rejected","status":"failed","error":{"code":"invalid_encrypted_content","message":"The encrypted content could not be verified"}}}`

func recoveryItems(t *testing.T, input string) []json.RawMessage {
	t.Helper()
	var items []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte("["+input+"]"), &items))
	return items
}

func compactionTestBody(input string, stream bool) []byte {
	body := autoContinuationBody(stream)
	body, _ = sjson.SetRawBytes(body, "input", []byte("["+input+"]"))
	return body
}

func closeRecoveryStore(t *testing.T, store *openAICompactionRecoveryStore) {
	t.Helper()
	t.Cleanup(func() {
		store.mu.Lock()
		defer store.mu.Unlock()
		for _, e := range store.entries {
			store.remove(e)
		}
	})
}

func TestCompactionRecoveryStoreRestoresWholeWindow(t *testing.T) {
	svc := &OpenAIGatewayService{}
	store := svc.compactionRecoveryStore()
	closeRecoveryStore(t, store)
	source := recoveryItems(t, autoContinuationBase+","+recoveryImage+","+autoContinuationCall+","+autoContinuationTool+","+recoveryRetained)
	output := recoveryItems(t, recoverySummary+","+recoveryRetained)
	require.True(t, store.put("scope", source, output))
	for _, prefix := range []string{"", autoContinuationBase + ","} {
		body := compactionTestBody(prefix+recoverySummary+","+recoveryRetained+","+recoveryUser, true)
		body, _ = sjson.SetBytes(body, "previous_response_id", "resp_stale")
		restored, ok := store.restore("scope", body, true)
		require.True(t, ok)
		expected := append(append([]json.RawMessage(nil), source...), json.RawMessage(recoveryUser))
		input, _ := json.Marshal(expected)
		require.JSONEq(t, string(input), gjson.GetBytes(restored, "input").Raw)
		require.False(t, gjson.GetBytes(restored, "previous_response_id").Exists())
		require.Equal(t, "xhigh", gjson.GetBytes(restored, "reasoning.effort").String())
	}
	_, ok := store.restore("scope", compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true), false)
	require.True(t, ok, "a later request must proactively replace the known invalid window")
	for _, input := range []string{recoverySummary, recoveryRetained + "," + recoverySummary, recoverySummary + "," + recoveryUser, recoveryUser + "," + recoverySummary + "," + recoveryRetained} {
		body := compactionTestBody(input, true)
		restored, ok := store.restore("scope", body, true)
		require.False(t, ok, input)
		require.Equal(t, body, restored, "unknown/edited window cannot be guessed")
	}
	_, ok = store.restore("other-key-thread-group", compactionTestBody(recoverySummary+","+recoveryRetained, true), true)
	require.False(t, ok)
}

func TestCompactionRecoveryCaptureRejectsIncompleteOrFailed(t *testing.T) {
	for _, scenario := range []string{"failed", "incomplete", "missing_index", "gap", "changed_terminal", "item_reference", "orphan_tool", "no_scope", "no_key", "valid_done_items", "valid_terminal"} {
		t.Run(scenario, func(t *testing.T) {
			svc := &OpenAIGatewayService{}
			store := svc.compactionRecoveryStore()
			closeRecoveryStore(t, store)
			c, _ := httpBridgeContext("capture")
			input := recoveryImage + "," + recoveryTrigger
			switch scenario {
			case "item_reference":
				input = recoveryImage + `,{"type":"item_reference","id":"msg_unknown"},` + recoveryTrigger
			case "orphan_tool":
				input = autoContinuationTool + "," + recoveryTrigger
			case "no_scope":
				c.Request.Header.Del("session-id")
			case "no_key":
				c.Set("api_key", &APIKey{})
			}
			svc.beginCompactionRecoveryTurn(c, compactionTestBody(input, true), false)
			done := []byte(`{"type":"response.output_item.done","output_index":0,"item":` + recoverySummary + `}`)
			terminal := autoContinuationEvent("resp_compact", recoverySummary)
			switch scenario {
			case "failed":
				terminal = []byte(recoveryCipherError)
			case "incomplete":
				terminal = []byte(`{"type":"response.incomplete","response":{"status":"incomplete","output":[` + recoverySummary + `]}}`)
			case "missing_index":
				done, _ = sjson.DeleteBytes(done, "output_index")
			case "gap":
				done, _ = sjson.SetBytes(done, "output_index", 1)
				terminal = autoContinuationEvent("resp_compact", "")
			case "changed_terminal":
				terminal = autoContinuationEvent("resp_compact", strings.ReplaceAll(recoverySummary, "recovery-test-cipher", "other-cipher"))
			case "valid_done_items":
				terminal = autoContinuationEvent("resp_compact", "")
			}
			svc.observeCompactionRecovery(c, done, "response.output_item.done")
			svc.observeCompactionRecovery(c, terminal, gjson.GetBytes(terminal, "type").String())
			expected := 0
			if strings.HasPrefix(scenario, "valid_") {
				expected = 1
			}
			require.Len(t, store.entries, expected)
		})
	}
}

// Codex V2 installs selected input items BEFORE its summary, with refreshed
// harness context. That installed window is not a contiguous input prefix.
func TestCompactionRecoveryCodexInstalledWindow(t *testing.T) {
	oldSetup := `{"type":"message","role":"developer","content":"original environment"}`
	newSetup := `{"type":"message","role":"developer","content":"current environment"}`
	for _, scenario := range []string{"retained_subset", "refreshed_context", "moved_context", "restart", "edited_user", "reordered_users", "missing_tool_call", "legacy_window"} {
		t.Run(scenario, func(t *testing.T) {
			svc := &OpenAIGatewayService{}
			store := svc.compactionRecoveryStore()
			dir := t.TempDir()
			require.NoError(t, store.startPersistence(dir, "installed-window-secret"))
			t.Cleanup(store.closePersistence)
			source := recoveryItems(t, oldSetup+","+recoveryImage+","+autoContinuationCall+","+autoContinuationTool+","+recoveryRetained)
			output := recoveryItems(t, recoverySummary)
			prefix := recoveryImage + "," + recoveryRetained
			switch scenario {
			case "refreshed_context", "restart":
				prefix = recoveryImage + "," + newSetup + "," + recoveryRetained
			case "moved_context":
				prefix = recoveryImage + "," + oldSetup + "," + recoveryRetained
			case "edited_user":
				prefix = strings.ReplaceAll(prefix, "retained-tail", "changed-user-text")
			case "reordered_users":
				prefix = recoveryRetained + "," + recoveryImage
			case "missing_tool_call":
				prefix += "," + strings.ReplaceAll(autoContinuationTool, "call_lookup", "unknown-call")
			case "legacy_window":
				output = recoveryItems(t, recoverySummary+","+recoveryRetained)
			}
			require.True(t, store.put("scope", source, output, "owner"))
			if scenario == "restart" {
				store.closePersistence()
				store = (&OpenAIGatewayService{}).compactionRecoveryStore()
				require.NoError(t, store.startPersistence(dir, "installed-window-secret"))
				t.Cleanup(store.closePersistence)
			}
			body := compactionTestBody(prefix+","+recoverySummary+","+recoveryUser, true)
			restored, ok := store.restore("scope", body, true, "owner")
			if scenario == "edited_user" || scenario == "reordered_users" || scenario == "missing_tool_call" || scenario == "legacy_window" {
				require.False(t, ok, "unverified history must never be substituted")
				require.Equal(t, body, restored)
				return
			}
			require.True(t, ok, "official retained messages are a subsequence, not a prefix")
			want := append([]json.RawMessage(nil), source...)
			if scenario == "refreshed_context" || scenario == "restart" {
				want = append(want, json.RawMessage(newSetup))
			}
			if scenario == "moved_context" {
				want = append(want, json.RawMessage(oldSetup))
			}
			want = append(want, json.RawMessage(recoveryUser))
			encoded, err := json.Marshal(want)
			require.NoError(t, err)
			require.JSONEq(t, string(encoded), gjson.GetBytes(restored, "input").Raw)
			require.True(t, AnalyzeToolCallOutputContextCoverageBytes(restored).ContextCoversAllCallIDs)
			require.NotContains(t, string(restored), "recovery-test-cipher")
		})
	}
}

func TestCompactionRecoveryLRUExpiryAndConcurrentIsolation(t *testing.T) {
	store := (&OpenAIGatewayService{}).compactionRecoveryStore()
	closeRecoveryStore(t, store)
	source := recoveryItems(t, recoveryImage)
	for i := range openAICompactionRecoveryPerScope + 1 {
		output := recoveryItems(t, strings.ReplaceAll(recoverySummary, "recovery-test-cipher", fmt.Sprint(i)))
		require.True(t, store.put("scope", source, output))
	}
	require.Len(t, store.entries, openAICompactionRecoveryPerScope)
	require.Nil(t, store.entries["scope:"+openAIEncryptedContentDigest("0")], "oldest, not newest, must be evicted")
	e := store.entries["scope:"+openAIEncryptedContentDigest("1")]
	e.expires = time.Now().Add(-time.Second)
	store.expire(e)
	require.NotContains(t, store.entries, e.key)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			scope := fmt.Sprint(i)
			store.put(scope, source, recoveryItems(t, recoverySummary))
			_, ok := store.restore(scope, compactionTestBody(recoverySummary+","+recoveryUser, true), true)
			require.True(t, ok)
		}(i)
	}
	wg.Wait()
	for i := range openAICompactionRecoveryMaxEntries + 3 {
		store.put(fmt.Sprint(i+100), source, recoveryItems(t, recoverySummary))
	}
	require.LessOrEqual(t, len(store.entries), openAICompactionRecoveryMaxEntries)
	require.LessOrEqual(t, store.bytes, openAICompactionRecoveryMaxBytes)
}

func TestCompactionRecoveryScopeAndRetryBudget(t *testing.T) {
	body := compactionTestBody(recoverySummary+","+recoveryUser, true)
	for _, scenario := range []string{"thread", "key", "group", "cancelled", "same"} {
		svc := &OpenAIGatewayService{}
		closeRecoveryStore(t, svc.compactionRecoveryStore())
		c, _ := httpBridgeContext("thread-one")
		c.Request.Header.Set("thread-id", "thread-one")
		c.Set("api_key", &APIKey{ID: 123, GroupID: new(int64)})
		svc.beginCompactionRecoveryTurn(c, compactionTestBody(recoveryImage+","+recoveryTrigger, true), false)
		svc.observeCompactionRecovery(c, autoContinuationEvent("resp_compact", recoverySummary), "response.completed")
		next, _ := httpBridgeContext("thread-one")
		next.Request.Header.Set("thread-id", "thread-one")
		group := int64(0)
		key := &APIKey{ID: 123, GroupID: &group}
		switch scenario {
		case "thread":
			next.Request.Header.Set("thread-id", "thread-two")
		case "key":
			key.ID++
		case "group":
			group++
		case "cancelled":
			ctx, cancel := context.WithCancel(next.Request.Context())
			cancel()
			next.Request = next.Request.WithContext(ctx)
		}
		next.Set("api_key", key)
		svc.beginCompactionRecoveryTurn(next, body, false)
		started := time.Now().Add(-3 * time.Second)
		compactionRecoveryStartTime(next, started)
		retry := svc.retryCompactionHistory(next)
		if scenario != "same" && scenario != "group" {
			require.Nil(t, retry, scenario)
			continue
		}
		require.NotNil(t, retry)
		previous := compactionRecoveryTurn(next)
		svc.beginCompactionRecoveryTurn(next, retry.body, false)
		resumeCompactionRecoveryTurn(next, previous)
		require.Nil(t, svc.retryCompactionHistory(next), "one recovery per turn")
		require.Equal(t, started, compactionRecoveryStartTime(next, time.Now()), "TTFT must include the failed attempt")
	}
}

func TestCompactionRecoveryNestedWindowAndActiveExpiry(t *testing.T) {
	store := (&OpenAIGatewayService{}).compactionRecoveryStore()
	closeRecoveryStore(t, store)
	summary2 := strings.ReplaceAll(recoverySummary, "recovery-test-cipher", "second-cipher")
	require.True(t, store.put("scope", recoveryItems(t, recoveryImage), recoveryItems(t, recoverySummary)))
	require.True(t, store.put("scope", recoveryItems(t, recoverySummary+","+recoveryRetained), recoveryItems(t, summary2)))
	entry := store.entries["scope:"+openAIEncryptedContentDigest("second-cipher")]
	entry.expires = time.Now().Add(time.Minute)
	_, ok := store.restore("scope", compactionTestBody(summary2+","+recoveryUser, true), false)
	require.False(t, ok, "valid summary is not rewritten")
	require.True(t, entry.expires.After(time.Now().Add(50*time.Minute)), "actual use refreshes the undo record")
	deadline := entry.expires
	_, ok = store.restore("scope", compactionTestBody(strings.ReplaceAll(summary2, "compaction", "compaction_summary"), true), false)
	require.False(t, ok)
	require.Equal(t, deadline, entry.expires, "unmatched output cannot extend retention")
	for _, force := range []bool{true, false} {
		restored, ok := store.restore("scope", compactionTestBody(summary2+","+recoveryUser, true), force)
		require.True(t, ok)
		require.JSONEq(t, "["+recoveryImage+","+recoveryRetained+","+recoveryUser+"]", gjson.GetBytes(restored, "input").Raw)
	}
	require.False(t, store.entries["scope:"+openAIEncryptedContentDigest("recovery-test-cipher")].invalid, "expanding an ancestor does not label that cipher invalid")
}

// Recovery must find verified history after a reconnect, without borrowing
// another key's history or accepting an edited compact window.
func TestCompactionRecoveryReconnectLookup(t *testing.T) {
	for _, scenario := range []string{"session_changed", "restart", "group_changed", "group_changed_restart", "legacy_group_changed_restart", "other_user", "other_key", "other_thread", "other_lane", "other_stream", "changed_cipher", "changed_tail", "legacy_owner", "ambiguous"} {
		t.Run(scenario, func(t *testing.T) {
			svc := &OpenAIGatewayService{}
			store := svc.compactionRecoveryStore()
			dir := t.TempDir()
			require.NoError(t, store.startPersistence(dir, "reconnect-test-secret"))
			t.Cleanup(store.closePersistence)
			account := &Account{ID: 42}
			seed, _ := httpBridgeContext("before-reconnect")
			svc.beginCompactionRecoveryTurn(seed, compactionTestBody(recoveryImage+","+autoContinuationCall+","+autoContinuationTool+","+recoveryRetained+","+recoveryTrigger, true), false, account)
			svc.observeCompactionRecovery(seed, autoContinuationEvent("resp_compact", recoverySummary+","+recoveryRetained), "response.completed")
			body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
			svc.beginCompactionRecoveryTurn(seed, body, false, account)
			svc.noteCompactionStreamFailure(seed, []byte(recoveryCipherError))
			if scenario == "legacy_group_changed_restart" {
				store.mu.Lock()
				for _, entry := range store.entries {
					entry.owner = fmt.Sprintf("%x", sha256.Sum256([]byte(`compaction-owner:123:0:"":"":""`)))
				}
				store.mu.Unlock()
			}
			if scenario == "legacy_owner" {
				store.mu.Lock()
				for _, entry := range store.entries {
					entry.owner = "" // Old-format records cannot prove cross-session ownership.
				}
				store.mu.Unlock()
			}
			if scenario == "ambiguous" {
				require.True(t, store.put("conflicting-scope", recoveryItems(t, recoveryUser+","+recoveryRetained), recoveryItems(t, recoverySummary+","+recoveryRetained), compactionRecoveryTurn(seed).owner))
			}
			if scenario == "restart" || strings.HasSuffix(scenario, "_restart") {
				store.closePersistence()
				svc = &OpenAIGatewayService{}
				require.NoError(t, svc.compactionRecoveryStore().startPersistence(dir, "reconnect-test-secret"))
				t.Cleanup(svc.compactionRecoveryStore().closePersistence)
			}
			c, _ := httpBridgeContext("after-reconnect")
			switch scenario {
			case "other_user":
				c.Set("api_key", &APIKey{ID: 123, UserID: 999})
			case "other_key":
				c.Set("api_key", &APIKey{ID: 999})
			case "group_changed", "group_changed_restart", "legacy_group_changed_restart":
				group := int64(99)
				c.Set("api_key", &APIKey{ID: 123, GroupID: &group})
			case "other_lane":
				c.Request.Header.Set(openAIWSTurnMetadataHeader, `{"request_kind":"memory"}`)
			case "other_thread":
				c.Request.Header.Set("thread-id", "unrelated-thread")
			case "other_stream":
				body, _ = sjson.SetBytes(body, "stream_id", "other-lane")
			case "changed_cipher":
				body = []byte(strings.ReplaceAll(string(body), "recovery-test-cipher", "unknown-cipher"))
			case "changed_tail":
				body = []byte(strings.ReplaceAll(string(body), "retained-tail", "edited-tail"))
			}
			restored := svc.beginCompactionRecoveryTurn(c, body, false, account)
			if scenario != "session_changed" && scenario != "restart" && !strings.Contains(scenario, "group_changed") {
				require.Equal(t, body, restored)
				require.Nil(t, svc.retryCompactionHistory(c))
				return
			}
			require.True(t, compactionRecoveryTurn(c).restored)
			require.NotContains(t, string(restored), "recovery-test-cipher")
			require.JSONEq(t, "["+recoveryImage+","+autoContinuationCall+","+autoContinuationTool+","+recoveryRetained+","+recoveryUser+"]", gjson.GetBytes(restored, "input").Raw)
			require.Equal(t, "xhigh", gjson.GetBytes(restored, "reasoning.effort").String())
		})
	}
}

// Active traffic must retain its undo even when a client presents a changed
// window (which remains ineligible for restoration), or a turn lasts past TTL.
func TestCompactionRecoveryActiveRetention(t *testing.T) {
	for _, scenario := range []string{"http", "ws_delta", "changed_window", "other_key", "in_flight_expiry", "capacity", "restart", "failed", "cancelled", "concurrent"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			svc := &OpenAIGatewayService{}
			store := svc.compactionRecoveryStore()
			require.NoError(t, store.startPersistence(dir, "active-recovery-secret"))
			t.Cleanup(store.closePersistence)
			seed, _ := httpBridgeContext("active-recovery")
			svc.beginCompactionRecoveryTurn(seed, compactionTestBody(recoveryImage+","+recoveryRetained+","+recoveryTrigger, true), false)
			svc.observeCompactionRecovery(seed, autoContinuationEvent("resp_compact", recoverySummary+","+recoveryRetained), "response.completed")
			var entry *openAICompactionRecoveryEntry
			store.mu.Lock()
			for _, e := range store.entries {
				entry = e
				e.expires = time.Now().Add(time.Minute)
			}
			store.mu.Unlock()
			require.NotNil(t, entry)
			c, _ := httpBridgeContext("active-recovery")
			t.Cleanup(func() { finishCompactionRecoveryTurn(c) })
			body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
			if scenario == "other_key" {
				c.Set("api_key", &APIKey{ID: 999})
			}
			if scenario == "changed_window" {
				body = []byte(strings.ReplaceAll(string(body), "retained-tail", "edited-tail"))
			}
			if scenario == "ws_delta" {
				delta, err := sjson.SetBytes(compactionTestBody(recoveryUser, true), "previous_response_id", "resp_compact")
				require.NoError(t, err)
				actual := svc.beginCompactionRecoveryWithReplay(c, delta, recoveryItems(t, recoverySummary+","+recoveryRetained), "resp_compact", true)
				require.Equal(t, delta, actual, "retaining undo must not expand the wire delta")
			} else {
				require.Equal(t, body, svc.beginCompactionRecoveryTurn(c, body, false))
			}
			store.mu.Lock()
			deadline := entry.expires
			store.mu.Unlock()
			if scenario == "other_key" {
				require.True(t, deadline.Before(time.Now().Add(2*time.Minute)), "another key cannot refresh retention")
				return
			}
			require.True(t, deadline.After(time.Now().Add(50*time.Minute)))
			if scenario == "changed_window" {
				require.Nil(t, svc.retryCompactionHistory(c), "renewal must not bypass window validation")
			}
			if scenario == "in_flight_expiry" {
				store.mu.Lock()
				entry.expires = time.Now().Add(-time.Second)
				store.mu.Unlock()
				store.expire(entry)
				store.mu.Lock()
				kept := store.entries[entry.key]
				store.mu.Unlock()
				require.Same(t, entry, kept, "active turn must retain its undo")
			}
			if scenario == "capacity" {
				for i := range openAICompactionRecoveryMaxEntries + 1 {
					require.True(t, store.put(fmt.Sprint(i), recoveryItems(t, recoveryImage), recoveryItems(t, recoverySummary)))
				}
				store.mu.Lock()
				kept, count := store.entries[entry.key], len(store.entries)
				store.mu.Unlock()
				require.Same(t, entry, kept, "evict idle snapshots before an in-flight undo")
				require.LessOrEqual(t, count, openAICompactionRecoveryMaxEntries)
			}
			// Even ordinary, non-compaction terminal events release the active hold.
			switch scenario {
			case "failed":
				svc.observeCompactionRecovery(c, []byte(recoveryCipherError), "response.failed")
			case "cancelled":
				finishCompactionRecoveryTurn(c)
			case "concurrent":
				other, _ := httpBridgeContext("active-recovery")
				svc.beginCompactionRecoveryTurn(other, body, false)
				finishCompactionRecoveryTurn(c)
				store.mu.Lock()
				entry.expires = time.Now().Add(-time.Second)
				store.mu.Unlock()
				store.expire(entry)
				store.mu.Lock()
				kept := store.entries[entry.key]
				store.mu.Unlock()
				finishCompactionRecoveryTurn(other)
				require.Same(t, entry, kept, "one completed turn must not release another turn's hold")
			default:
				svc.observeCompactionRecovery(c, autoContinuationEvent("resp_next", recoveryUser), "response.completed")
			}
			if scenario == "restart" {
				store.closePersistence()
				loaded := (&OpenAIGatewayService{}).compactionRecoveryStore()
				require.NoError(t, loaded.startPersistence(dir, "active-recovery-secret"))
				t.Cleanup(loaded.closePersistence)
				next, ok := loaded.restore(compactionRecoveryTurn(c).scope, body, true, compactionRecoveryTurn(c).owner)
				require.True(t, ok)
				require.Contains(t, string(next), "7391")
				return
			}
			store.mu.Lock()
			entry.expires = time.Now().Add(-time.Second)
			store.mu.Unlock()
			store.expire(entry)
			store.mu.Lock()
			kept := store.entries[entry.key]
			store.mu.Unlock()
			require.Nil(t, kept, "completed traffic must not pin snapshots forever")
		})
	}
}

func TestCompactionRecoveryNestedSnapshotSurvivesAncestorEviction(t *testing.T) {
	store := (&OpenAIGatewayService{}).compactionRecoveryStore()
	dir := t.TempDir()
	require.NoError(t, store.startPersistence(dir, "nested-test-secret"))
	t.Cleanup(store.closePersistence)
	input := recoveryImage + "," + autoContinuationCall + "," + autoContinuationTool
	var summary string
	for i := range openAICompactionRecoveryPerScope + 3 {
		summary = strings.ReplaceAll(recoverySummary, "recovery-test-cipher", fmt.Sprintf("nested-%d", i))
		require.True(t, store.put("scope", recoveryItems(t, input), recoveryItems(t, summary)))
		input = summary + "," + recoveryRetained
	}
	store.mu.Lock()
	for _, entry := range store.entries {
		require.False(t, entry.invalid, "flattening a backup must not invalidate live ciphertext")
		if entry.key != "scope:"+openAIEncryptedContentDigest(fmt.Sprintf("nested-%d", openAICompactionRecoveryPerScope+2)) {
			store.remove(entry)
		}
	}
	store.mu.Unlock()
	store.closePersistence()
	loaded := (&OpenAIGatewayService{}).compactionRecoveryStore()
	require.NoError(t, loaded.startPersistence(dir, "nested-test-secret"))
	t.Cleanup(loaded.closePersistence)
	require.Len(t, loaded.entries, 1)
	restored, ok := loaded.restore("scope", compactionTestBody(summary+","+recoveryUser, true), true)
	require.True(t, ok)
	require.Contains(t, string(restored), "7391")
	require.Contains(t, string(restored), "data:image/png;base64,AAA=")
	require.Contains(t, string(restored), "lookup result")
	require.NotContains(t, gjson.GetBytes(restored, "input").Raw, "encrypted_content")
	require.Equal(t, openAICompactionRecoveryPerScope+2, strings.Count(string(restored), "retained-tail"))
}

func TestCompactionRecoveryDoesNotReplayAfterClientOutput(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprint(passthrough), func(t *testing.T) {
			svc, account, upstream := httpBridgeFixture(t, "http://unused.invalid")
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			account.Type = AccountTypeAPIKey
			account.Credentials = map[string]any{"api_key": "key"}
			account.Extra = map[string]any{"openai_passthrough": passthrough}
			c, _ := httpBridgeContext("no-repeat")
			svc.beginCompactionRecoveryTurn(c, compactionTestBody(recoveryImage+","+recoveryTrigger, true), false)
			svc.observeCompactionRecovery(c, autoContinuationEvent("resp_compact", recoverySummary), "response.completed")
			upstream.responses = []*http.Response{{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"delta\":\"already-delivered\"}\n\ndata: " + recoveryCipherError + "\n\n"))}}
			c, rec := httpBridgeContext("no-repeat")
			_, _ = svc.Forward(c.Request.Context(), c, account, compactionTestBody(recoverySummary+","+recoveryUser, true))
			require.Equal(t, 1, upstream.callCount, "cannot duplicate a partially delivered response")
			require.Contains(t, rec.Body.String(), "already-delivered")
			require.Empty(t, compactionRecoveryTurn(c).body, "release temporary plaintext at turn end")
			for _, e := range svc.compactionRecoveryStore().entries {
				require.True(t, e.invalid, "remember rejection for the next explicit request")
			}
		})
	}
}

func TestCompactionRecoveryNativeIncrementalCapture(t *testing.T) {
	svc := &OpenAIGatewayService{}
	closeRecoveryStore(t, svc.compactionRecoveryStore())
	replay := recoveryItems(t, autoContinuationBase+","+recoveryImage+","+autoContinuationCall)
	body := compactionTestBody(autoContinuationTool+","+recoveryTrigger, true)
	body, _ = sjson.SetBytes(body, "previous_response_id", "resp_history")
	for _, complete := range []bool{false, true} {
		c, _ := httpBridgeContext("native-compact")
		actual := svc.beginCompactionRecoveryWithReplay(c, body, replay, "resp_history", complete)
		require.Equal(t, body, actual, "normal wire must stay incremental")
		svc.observeCompactionRecovery(c, autoContinuationEvent("resp_compact", recoverySummary), "response.completed")
		if !complete {
			require.Empty(t, svc.compactionRecoveryStore().entries)
		}
	}
	c, _ := httpBridgeContext("native-compact")
	svc.beginCompactionRecoveryTurn(c, compactionTestBody(recoverySummary+","+recoveryUser, true), false)
	retry := svc.retryCompactionHistory(c)
	require.NotNil(t, retry)
	require.Contains(t, string(retry.body), "7391")
	require.Contains(t, string(retry.body), "lookup result")
	coverage := AnalyzeToolCallOutputContextCoverageBytes(retry.body)
	require.True(t, coverage.ContextCoversAllCallIDs)
	require.NotContains(t, string(retry.body), "compaction_trigger")
}

func TestCompactionRecoveryLegacyHTTPAndAccountSwitch(t *testing.T) {
	svc, account, upstream := httpBridgeFixture(t, "http://unused.invalid")
	closeRecoveryStore(t, svc.compactionRecoveryStore())
	account.Type = AccountTypeAPIKey
	account.Credentials = map[string]any{"api_key": "old-key"}
	response := func(status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
	}
	upstream.responses = []*http.Response{
		response(200, `{"object":"response.compaction","id":"cmp_compact","output":[`+recoverySummary+","+recoveryRetained+`],"usage":{"input_tokens":10,"output_tokens":2}}`),
		response(400, `{"error":{"code":"invalid_encrypted_content","message":"Cannot decrypt summary"}}`),
		response(200, gjson.GetBytes(autoContinuationEvent("resp_recovered", autoContinuationCall), "response").Raw),
	}
	c, _ := httpBridgeContext("cross-account")
	c.Request.URL.Path = "/v1/responses/compact"
	_, err := svc.Forward(c.Request.Context(), c, account, compactionTestBody(recoveryImage+","+recoveryRetained, false))
	require.NoError(t, err)
	require.Len(t, svc.compactionRecoveryStore().entries, 1)
	// The undo record is isolated by the client key/thread, not the old upstream
	// account. Rebuilding must apply the newly selected account's credentials.
	account.ID++
	account.Credentials = map[string]any{"api_key": "new-key"}
	c, rec := httpBridgeContext("cross-account")
	_, err = svc.Forward(c.Request.Context(), c, account, compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, false))
	require.NoError(t, err)
	require.Contains(t, rec.Body.String(), "resp_recovered")
	require.Equal(t, 3, upstream.callCount)
	require.Equal(t, "Bearer new-key", upstream.reqs[2].Header.Get("Authorization"))
	require.Contains(t, string(upstream.bodies[2]), "7391")
	require.Equal(t, 1, strings.Count(string(upstream.bodies[2]), "retained-tail"))
}

func TestCompactionAccountSwitchCanRevalidateCipherWithoutUndoRecord(t *testing.T) {
	svc, account, upstream := httpBridgeFixture(t, "http://unused.invalid")
	account.Type = AccountTypeAPIKey
	account.Credentials = map[string]any{"api_key": "test-key"}
	body := compactionTestBody(recoverySummary+","+recoveryUser, false)
	c, _ := httpBridgeContext("cipher-account-switch")
	session := svc.GenerateSessionHash(c, body)
	svc.markOpenAIWSInvalidEncryptedContentLineage(0, session, collectOpenAIEncryptedContentDigestsRaw(body), account.ID+1)
	result, err := svc.Forward(c.Request.Context(), c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, upstream.callCount)
	require.Contains(t, string(upstream.bodies[0]), "recovery-test-cipher", "keep the complete summary when revalidating with another account")
}

func TestCompactionErrorsCommitExactlyOneJSONResponse(t *testing.T) {
	for _, recompact := range []bool{false, true} {
		c, rec := httpBridgeContext("terminal-error")
		if recompact {
			require.Error(t, writeOpenAIRecompactionError(c, fmt.Errorf("cannot recover history")))
		} else {
			require.ErrorIs(t, writeOpenAIEncryptedCompactionError(c), errOpenAIEncryptedCompactionContext)
		}
		require.True(t, IsResponseCommitted(c), "handler must not append response.failed after this terminal JSON")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.True(t, json.Valid(rec.Body.Bytes()))
		require.NotEmpty(t, gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
	}
}

func TestNativeWSCooldownUsesHTTPAndKeepsToolHistory(t *testing.T) {
	for _, mode := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough} {
		t.Run(mode, func(t *testing.T) {
			requests := make(chan []byte, 2)
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
					http.Error(w, "WS must stay in cooldown", http.StatusBadRequest)
					return
				}
				raw, _ := io.ReadAll(r.Body)
				requests <- raw
				n := calls.Add(1)
				id := fmt.Sprintf("resp_http_%d", n)
				output := autoContinuationCall
				if n == 2 {
					output = autoContinuationAnswer
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":%q,\"status\":\"in_progress\"}}\n\n", id)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent(id, output))
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": mode}
			first := autoContinuationBody(true)
			c, _ := httpBridgeContext("recovery-e2e")
			scope := resolveOpenAIWSSessionScope(c, account, first)
			svc.markOpenAIWSSessionDisabled(scope, "upstream_policy_close")
			second := []byte(`{"model":"gpt-6-astra","store":false,"stream":true,"previous_response_id":"resp_http_1","input":[` + autoContinuationTool + `]}`)
			runCompactionRecoveryNativeClient(t, svc, account, [][]byte{first, second})
			require.Equal(t, int32(2), calls.Load())
			<-requests
			followup := <-requests
			require.Empty(t, gjson.GetBytes(followup, "previous_response_id").String())
			require.Contains(t, string(followup), "function_call_output")
			require.True(t, AnalyzeToolCallOutputContextCoverageBytes(followup).ContextCoversAllCallIDs)
			require.Equal(t, "upstream_policy_close", svc.openAIWSSessionDisabledReason(scope))

		})
	}
}

// Replace only the network destination, preserving the production request and
// streaming implementation. Tests never reach an external upstream.
type compactionRecoveryLocalHTTP struct{ base *url.URL }

func (u compactionRecoveryLocalHTTP) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	copy := req.Clone(req.Context())
	copy.URL.Scheme, copy.URL.Host, copy.Host = u.base.Scheme, u.base.Host, u.base.Host
	return http.DefaultClient.Do(copy)
}
func (u compactionRecoveryLocalHTTP) DoWithTLS(req *http.Request, proxy string, id int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxy, id, concurrency)
}

func TestCompactionRecoveryEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"http", "http_passthrough", "http_to_ws", "http_group_restart", "http_to_ws_group_restart", "ws", "ws_to_http", "ws_delta", "ws_to_http_delta", "ws_delta_compact", "ws_to_http_delta_compact", "ws_passthrough", "ws_passthrough_delta", "ws_passthrough_delta_compact"} {
		for _, failure := range []string{"error", "response.failed"} {
			t.Run(path+"/"+failure, func(t *testing.T) {
				crossGroup := strings.HasSuffix(path, "_group_restart")
				path := strings.TrimSuffix(path, "_group_restart")
				requests := make(chan []byte, 12)
				var rejected atomic.Int32
				events := func(raw []byte) [][]byte {
					requests <- append([]byte(nil), raw...)
					created := func(id string) []byte {
						return []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress"}}`, id))
					}
					if HasCompactionTriggerInInput(raw) {
						return [][]byte{created("resp_compact"), autoContinuationEvent("resp_compact", recoverySummary+","+recoveryRetained)}
					}
					if strings.Contains(string(raw), "recovery-test-cipher") || gjson.GetBytes(raw, "previous_response_id").String() == "resp_compact" {
						rejected.Add(1)
						errEvent := []byte(recoveryCipherError)
						if failure == "error" {
							errEvent = []byte(`{"type":"error","error":{"code":"invalid_encrypted_content","message":"Cannot decrypt compaction"}}`)
						}
						if failure == "error" {
							return [][]byte{created("resp_rejected"), errEvent, []byte(recoveryCipherError)}
						}
						return [][]byte{created("resp_rejected"), errEvent}
					}
					if strings.Contains(string(raw), "lookup result") {
						return [][]byte{created("resp_tool_done"), autoContinuationEvent("resp_tool_done", autoContinuationAnswer)}
					}
					return [][]byte{created("resp_recovered"), autoContinuationEvent("resp_recovered", autoContinuationCall)}
				}
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if websocket.IsWebSocketUpgrade(r) {
						conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
						if err != nil {
							return
						}
						defer conn.Close()
						for {
							_, raw, err := conn.ReadMessage()
							if err != nil {
								return
							}
							for _, event := range events(raw) {
								if conn.WriteMessage(websocket.TextMessage, event) != nil {
									return
								}
							}
						}
					}
					raw, _ := io.ReadAll(r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					for _, event := range events(raw) {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
						w.(http.Flusher).Flush()
					}
				}))
				defer upstream.Close()
				svc, account, _ := httpBridgeFixture(t, upstream.URL)
				base, _ := url.Parse(upstream.URL)
				svc.httpUpstream = compactionRecoveryLocalHTTP{base}
				closeRecoveryStore(t, svc.compactionRecoveryStore())
				var recoveryDir string
				if crossGroup {
					recoveryDir = t.TempDir()
					require.NoError(t, svc.compactionRecoveryStore().startPersistence(recoveryDir, "cross-group-e2e"))
					t.Cleanup(svc.compactionRecoveryStore().closePersistence)
				}
				if path != "http_to_ws" {
					account.Type = AccountTypeAPIKey
					account.Credentials = map[string]any{"api_key": "sk-test"}
				}
				account.Status, account.Schedulable = StatusActive, true
				if path == "http_passthrough" {
					account.Extra = map[string]any{"openai_passthrough": true}
				}
				native := strings.HasPrefix(path, "ws")
				svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
				if native {
					svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
					mode := OpenAIWSIngressModeCtxPool
					if strings.HasPrefix(path, "ws_to_http") {
						mode = OpenAIWSIngressModeHTTPBridge
					}
					if strings.HasPrefix(path, "ws_passthrough") {
						mode = OpenAIWSIngressModePassthrough
						svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
					}
					account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": mode}
				}
				bodies := [][]byte{
					compactionTestBody(autoContinuationBase+","+recoveryImage+","+recoveryRetained+","+recoveryTrigger, true),
					compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true),
					compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser+","+autoContinuationCall+","+autoContinuationTool, true),
				}
				if strings.HasSuffix(path, "_delta") {
					bodies[1] = compactionTestBody(recoveryUser, true)
					bodies[1], _ = sjson.SetBytes(bodies[1], "previous_response_id", "resp_compact")
				}
				if strings.HasSuffix(path, "_delta_compact") {
					bodies[0] = compactionTestBody(autoContinuationBase+","+recoveryImage+","+recoveryRetained, true)
					bodies[1] = compactionTestBody(autoContinuationTool+","+recoveryTrigger, true)
					bodies[1], _ = sjson.SetBytes(bodies[1], "previous_response_id", "resp_recovered")
					bodies[2] = compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
				}
				failedTurn := 1
				if strings.HasSuffix(path, "_delta_compact") {
					failedTurn = 2
				}
				// The client observes the failure, then explicitly starts a new response.
				bodies = append(bodies[:failedTurn+1], append([][]byte{bodies[failedTurn]}, bodies[failedTurn+1:]...)...)
				if native {
					runCompactionRecoveryNativeClientFailures(t, svc, account, bodies, map[int]bool{failedTurn: true})
				} else {
					for i, body := range bodies {
						c, rec := httpBridgeContext("recovery-e2e")
						if crossGroup {
							group := int64(42)
							if i == 0 {
								group = 97
							}
							c.Set("api_key", &APIKey{ID: 123, UserID: 484, GroupID: &group})
							if i == 1 {
								svc.compactionRecoveryStore().closePersistence()
								svc.openaiCompactionRecoveryOnce = sync.Once{}
								svc.openaiCompactionRecovery = nil
								require.NoError(t, svc.compactionRecoveryStore().startPersistence(recoveryDir, "cross-group-e2e"))
								t.Cleanup(svc.compactionRecoveryStore().closePersistence)
							}
						}
						result, err := svc.Forward(c.Request.Context(), c, account, body)
						if i == failedTurn {
							require.Contains(t, rec.Body.String(), "invalid_encrypted_content")
							require.NotContains(t, rec.Body.String(), "resp_recovered")
							require.Len(t, requests, i+1, "failure must not cause a hidden retry")
							continue
						}
						require.NoError(t, err, "turn %d: %s", i, rec.Body.String())
						require.NotNil(t, result)
						require.NotContains(t, rec.Body.String(), "response.failed")
						if i == failedTurn+1 {
							require.Contains(t, rec.Body.String(), "resp_recovered")
							require.NotContains(t, rec.Body.String(), "resp_rejected")
						}
						if i == failedTurn+2 && path == "http_to_ws" {
							require.True(t, c.GetBool("openai_ws_auto_incremental_succeeded"), c.GetString("openai_ws_auto_incremental_reason"))
						}
					}
				}
				require.Equal(t, int32(1), rejected.Load(), "next turn must not submit the invalid summary again")
				require.Len(t, requests, 4, "compact + visible rejection + explicit new response + tool continuation")
				if strings.HasSuffix(path, "_delta_compact") {
					<-requests // initial generation, before the incremental compact
					compact, rejected, restored := <-requests, <-requests, <-requests
					require.True(t, HasCompactionTriggerInInput(compact))
					require.Contains(t, string(rejected), "recovery-test-cipher")
					require.Contains(t, string(restored), "7391")
					require.Contains(t, string(restored), "data:image/png;base64,AAA=")
					require.Contains(t, string(restored), "lookup result")
					require.True(t, AnalyzeToolCallOutputContextCoverageBytes(restored).ContextCoversAllCallIDs)
					require.NotContains(t, string(restored), "compaction_trigger")
					require.NotContains(t, string(restored), "recovery-test-cipher")
					require.Equal(t, 1, strings.Count(string(restored), "retained-tail"))
					return
				}
				compact, bad, restored, next := <-requests, <-requests, <-requests, <-requests
				require.Contains(t, string(compact), "7391")
				if path == "ws_delta" || path == "ws_passthrough_delta" {
					require.Equal(t, "resp_compact", gjson.GetBytes(bad, "previous_response_id").String())
				} else {
					require.Contains(t, string(bad), "recovery-test-cipher")
				}
				require.NotContains(t, string(restored), "recovery-test-cipher")
				require.Contains(t, string(restored), "7391")
				require.Contains(t, string(restored), "data:image/png;base64,AAA=")
				require.Equal(t, 1, strings.Count(string(restored), "retained-tail"), "retained compact messages cannot be duplicated")
				require.Empty(t, gjson.GetBytes(restored, "previous_response_id").String())
				for _, raw := range [][]byte{restored, next} {
					require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
					require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
					require.Equal(t, "reasoning.encrypted_content", gjson.GetBytes(raw, "include.0").String())
				}
				if path == "http_to_ws" {
					require.Equal(t, "resp_recovered", gjson.GetBytes(next, "previous_response_id").String())
					require.JSONEq(t, "["+autoContinuationTool+"]", gjson.GetBytes(next, "input").Raw)
				}
			})
		}
	}
}

func runCompactionRecoveryNativeClient(t *testing.T, svc *OpenAIGatewayService, account *Account, bodies [][]byte, customize ...func(*OpenAIWSIngressHooks)) {
	runCompactionRecoveryNativeClientFailures(t, svc, account, bodies, nil, customize...)
}

func runCompactionRecoveryNativeClientFailures(t *testing.T, svc *OpenAIGatewayService, account *Account, bodies [][]byte, failures map[int]bool, customize ...func(*OpenAIWSIngressHooks)) {
	t.Helper()
	runCompactionRecoveryClientSession(t, svc, account, "recovery-e2e", bodies, failures, customize...)
}

func runCompactionRecoveryClientSession(t *testing.T, svc *OpenAIGatewayService, account *Account, session string, bodies [][]byte, failures map[int]bool, customize ...func(*OpenAIWSIngressHooks)) {
	t.Helper()
	errs := make(chan error, 1)
	var before, after atomic.Int32
	var beforeTurns, afterTurns []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			errs <- err
			return
		}
		defer conn.CloseNow()
		_, first, err := conn.Read(r.Context())
		if err != nil {
			errs <- err
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		c.Set("api_key", &APIKey{ID: 123})
		hooks := &OpenAIWSIngressHooks{
			BeforeTurn: func(n int) error { before.Add(1); beforeTurns = append(beforeTurns, n); return nil },
			AfterTurn:  func(n int, _ *OpenAIForwardResult, _ error) { after.Add(1); afterTurns = append(afterTurns, n) },
		}
		if account.ResolveOpenAIResponsesWebSocketV2Mode(svc.cfg.Gateway.OpenAIWS.IngressModeDefault) == OpenAIWSIngressModePassthrough &&
			svc.openAIWSSessionDisabledReason(resolveOpenAIWSSessionScope(c, account, first)) == "" {
			before.Add(1) // First passthrough turn is admitted by the handshake.
			beforeTurns = append(beforeTurns, 1)
		}
		for _, apply := range customize {
			apply(hooks)
		}
		errs <- svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "sk-test", first, hooks)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &coderws.DialOptions{HTTPHeader: http.Header{"Session-Id": {session}}})
	require.NoError(t, err)
	defer conn.CloseNow()
	for i, body := range bodies {
		body, _ = sjson.SetBytes(body, "type", "response.create")
		require.NoError(t, conn.Write(ctx, coderws.MessageText, body))
		created := 0
		for {
			_, message, err := conn.Read(ctx)
			require.NoError(t, err, "turn %d", i)
			if lane := gjson.GetBytes(body, "stream_id").String(); lane != "" {
				require.Equal(t, lane, gjson.GetBytes(message, "stream_id").String(), "response event must belong to its originating lane")
			}
			kind := gjson.GetBytes(message, "type").String()
			if !failures[i] {
				require.NotEqual(t, "error", kind, string(message))
				require.NotEqual(t, "response.failed", kind, string(message))
			}
			if kind == "response.failed" {
				require.True(t, failures[i])
				break
			}
			if kind == "response.created" {
				created++
			}
			if kind == "response.completed" {
				break
			}
		}
		require.Equal(t, 1, created, "each explicit response has its own lifecycle")
	}
	_ = conn.Close(coderws.StatusNormalClosure, "done")
	select {
	case err := <-errs:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("gateway did not exit")
	}
	require.Equal(t, int32(len(bodies)), before.Load(), "recovery cannot repeat admission")
	require.Equal(t, int32(len(bodies)), after.Load(), "bill only completed client turns")
	wantTurns := make([]int, len(bodies))
	for i := range wantTurns {
		wantTurns[i] = i + 1
	}
	require.Equal(t, wantTurns, beforeTurns, "preserve admission turn numbers across relay recovery")
	require.Equal(t, wantTurns, afterTurns, "preserve billing turn numbers across relay recovery")
}

func TestCompactionRecoveryNativeKnownInvalidBeforeUpstream(t *testing.T) {
	for _, scenario := range []string{"ws", "http_bridge", "ws_restart", "http_bridge_restart", "passthrough_restart", "ws_group_restart", "http_bridge_group_restart", "passthrough_group_restart"} {
		t.Run(scenario, func(t *testing.T) {
			requests := make(chan []byte, 4)
			events := [][]byte{[]byte(`{"type":"response.created","response":{"id":"resp_ok","status":"in_progress"}}`), autoContinuationEvent("resp_ok", autoContinuationAnswer)}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
					conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					_, raw, err := conn.ReadMessage()
					if err != nil {
						return
					}
					requests <- raw
					for _, event := range events {
						_ = conn.WriteMessage(websocket.TextMessage, event)
					}
					_, _, _ = conn.ReadMessage()
					return
				}
				raw, _ := io.ReadAll(r.Body)
				requests <- raw
				w.Header().Set("Content-Type", "text/event-stream")
				for _, event := range events {
					_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
				}
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			restart := strings.HasSuffix(scenario, "_restart")
			dir := t.TempDir()
			if restart {
				require.NoError(t, svc.compactionRecoveryStore().startPersistence(dir, "test-secret"))
			}
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
			svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			account.Type = AccountTypeAPIKey
			account.Status, account.Schedulable = StatusActive, true
			account.Credentials = map[string]any{"api_key": "sk-test"}
			mode := OpenAIWSIngressModeCtxPool
			if strings.HasPrefix(scenario, "http_bridge") {
				mode = OpenAIWSIngressModeHTTPBridge
			}
			if strings.HasPrefix(scenario, "passthrough") {
				mode = OpenAIWSIngressModePassthrough
				svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
			}
			account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": mode}
			c, _ := httpBridgeContext("recovery-e2e")
			if strings.Contains(scenario, "group_restart") {
				group := int64(97)
				c.Set("api_key", &APIKey{ID: 123, GroupID: &group})
			}
			svc.beginCompactionRecoveryTurn(c, compactionTestBody(recoveryImage+","+recoveryTrigger, true), false)
			svc.observeCompactionRecovery(c, autoContinuationEvent("resp_compact", recoverySummary), "response.completed")
			body := compactionTestBody(recoverySummary+","+recoveryUser, true)
			if restart {
				svc.beginCompactionRecoveryTurn(c, body, false)
				svc.noteCompactionStreamFailure(c, []byte(recoveryCipherError))
				svc.compactionRecoveryStore().closePersistence()
				svc.openaiCompactionRecoveryOnce = sync.Once{}
				svc.openaiCompactionRecovery = nil
				require.NoError(t, svc.compactionRecoveryStore().startPersistence(dir, "test-secret"))
				t.Cleanup(svc.compactionRecoveryStore().closePersistence)
			} else {
				scope, _ := resolveOpenAIWSExecutionScope(c, body, 123)
				svc.markOpenAIWSInvalidEncryptedContentLineage(0, scope, []string{openAIEncryptedContentDigest("recovery-test-cipher")}, account.ID)
			}
			runCompactionRecoveryNativeClient(t, svc, account, [][]byte{body})
			require.Len(t, requests, 1, "known-invalid summary should be restored before any upstream attempt")
			sent := <-requests
			require.Contains(t, string(sent), "7391")
			require.NotContains(t, string(sent), "recovery-test-cipher")
		})
	}
}
