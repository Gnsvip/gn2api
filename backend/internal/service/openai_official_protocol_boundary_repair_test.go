package service

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestOfficialBoundaryRepairRecoveryGuards(t *testing.T) {
	for _, envelope := range []string{"error", "response.failed"} {
		for _, scenario := range []string{"missing_call", "wrong_call_id", "wrong_tool_type", "duplicate_result", "item_reference", "unknown_parent", "disabled", "after_output", "rejected_again"} {
			t.Run(envelope+"/"+scenario, func(t *testing.T) {
				requests := make(chan []byte, 8)
				svc, account := officialFollowupAuditFixture(t, OpenAIWSIngressModeCtxPool, func(raw []byte, n int) []byte {
					requests <- append([]byte(nil), raw...)
					if n == 1 {
						output := autoContinuationCall
						if scenario == "missing_call" {
							output = autoContinuationAnswer
						}
						return autoContinuationEvent("resp_guard", output)
					}
					errBody := `{"code":"previous_response_not_found","type":"invalid_request_error","message":"expired parent"}`
					if envelope == "response.failed" {
						return []byte(`{"type":"response.failed","response":{"id":"resp_failed","status":"failed","error":` + errBody + `,"output":[]}}`)
					}
					return []byte(`{"type":"error","error":` + errBody + `}`)
				}, func(_ []byte, n int) [][]byte {
					if scenario == "after_output" && n == 2 {
						return [][]byte{[]byte(`{"type":"response.output_text.delta","delta":"already visible"}`)}
					}
					return nil
				})
				svc.cfg.Gateway.OpenAIWS.IngressPreviousResponseRecoveryEnabled = scenario != "disabled"
				terminal := ""
				visible := false
				officialBoundaryAuditClient(t, svc, account, func(ctx context.Context, conn *coderws.Conn) {
					officialFollowupExchange(t, ctx, conn, compactionTestBody(autoContinuationBase, true))
					input := autoContinuationTool
					switch scenario {
					case "wrong_call_id":
						raw, _ := sjson.Set(input, "call_id", "other_call")
						input = raw
					case "wrong_tool_type":
						raw, _ := sjson.Set(input, "type", "custom_tool_call_output")
						input = raw
					case "duplicate_result":
						input += "," + input
					case "item_reference":
						input += `,{"type":"item_reference","id":"msg_old_account"}`
					}
					next := compactionTestBody(input, true)
					next, _ = sjson.SetBytes(next, "type", "response.create")
					parent := "resp_guard"
					if scenario == "unknown_parent" {
						parent = "resp_other_connection"
					}
					next, _ = sjson.SetBytes(next, "previous_response_id", parent)
					require.NoError(t, conn.Write(ctx, coderws.MessageText, next))
					for {
						_, raw, err := conn.Read(ctx)
						if err != nil {
							terminal = "socket_closed"
							return
						}
						event := gjson.GetBytes(raw, "type").String()
						visible = visible || event == "response.output_text.delta"
						if event == "error" || event == "response.failed" || event == "response.completed" {
							terminal = event
							return
						}
					}
				})
				require.NotEqual(t, "response.completed", terminal)
				expected := 2
				if scenario == "rejected_again" {
					expected = 3
				}
				if scenario == "after_output" {
					require.True(t, visible)
				}
				require.Len(t, requests, expected, "unsafe or already delivered turns must not be replayed; a recoverable turn retries at most once")
			})
		}
	}
}

func TestOfficialBoundaryRepairHistoryRetentionAllocation(t *testing.T) {
	const turns = 128
	payloads := make([][]byte, turns)
	for i := range payloads {
		payloads[i] = buildReplayTurnPayload(i+1, 10*1024)
	}
	cache := openAIWSResponseHistory{limit: 8 * 1024 * 1024}
	var last openAIWSResponseHistoryState
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i, payload := range payloads {
		items, exists, err := openAIWSExtractNormalizedInputSequence(payload)
		require.NoError(t, err)
		require.True(t, exists)
		id := fmt.Sprintf("resp_allocation_%d", i)
		state := cache.put(id, openAIWSResponseHistoryState{responseID: id, replay: items, failover: items, complete: true})
		if i > 0 {
			require.Same(t, &last.replay[0][0], &state.replay[0][0], "unchanged archived history remains shared")
		}
		require.Same(t, &state.replay[i][0], &state.failover[i][0], "replay and failover share newly retained bytes")
		require.NotSame(t, &items[i][0], &state.replay[i][0], "archive must release the enclosing request allocation")
		last = state
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("archive_total_allocated=%d turns=%d retained_accounting=%d", allocated, turns, cache.bytes)
	require.Less(t, allocated, uint64(32*1024*1024), "copying full history each turn would allocate at least 84 MiB")
	require.LessOrEqual(t, cache.bytes, cache.limit)
}

func TestOfficialBoundaryRepairHistoryOwnsMetadataAndEvicts(t *testing.T) {
	cache := openAIWSResponseHistory{limit: 4096}
	backing := []byte(strings.Repeat("x", 1024*1024))
	small := backing[100:110]
	state := cache.put("resp_owned", openAIWSResponseHistoryState{
		responseID: "resp_owned", complete: true, replay: []json.RawMessage{small},
		seed: small, strict: &openAIWSIngressPreviousTurnStrictState{nonInputComparable: small},
	})
	for _, retained := range [][]byte{state.replay[0], state.seed, state.strict.nonInputComparable} {
		require.Equal(t, small, retained)
		require.NotSame(t, &small[0], &retained[0])
		require.Less(t, cap(retained), 64)
	}
	cache.take("resp_owned")
	require.Zero(t, cache.bytes)
	require.Empty(t, cache.states)
	for _, id := range cache.order[:cap(cache.order)] {
		require.Empty(t, id, "evicted IDs must not remain reachable in the order backing array")
	}
}

func TestOfficialBoundaryRepairImplicitReferenceNeedsRealCall(t *testing.T) {
	body := compactionTestBody(`{"type":"item_reference","id":"call_lookup"},`+autoContinuationTool, true)
	for _, tc := range []struct {
		name     string
		history  string
		complete bool
		want     bool
	}{
		{"actual_call", autoContinuationCall, true, true},
		{"reference_only", `{"type":"item_reference","id":"call_lookup"}`, true, false},
		{"unrelated_history", autoContinuationBase, true, false},
		{"incomplete_history", autoContinuationCall, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, openAIWSImplicitToolContinuation(body, []json.RawMessage{json.RawMessage(tc.history)}, tc.complete))
		})
	}
	for _, field := range []string{"previous_response_id", "stream_id"} {
		named, _ := sjson.SetBytes(body, field, "explicit")
		require.False(t, openAIWSImplicitToolContinuation(named, []json.RawMessage{json.RawMessage(autoContinuationCall)}, true))
	}
}
