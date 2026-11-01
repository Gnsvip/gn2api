//go:build linux

package main

import (
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOfficial166CustomCAH2Lifecycle(t *testing.T) {
	dir := "../../internal/pkg/codexwire/testdata/golden/0.166.0/lifecycle-custom-ca"
	for _, name := range []string{"ws_initial", "ws_tool_loop", "http_fallback_tool_loop", "http_unauthorized_refresh", "ws_unauthorized_refresh", "ws_compaction"} {
		t.Run(name, func(t *testing.T) {
			rows, err := readRecordMap(filepath.Join(dir, name+".jsonl"))
			require.NoError(t, err)
			records := make([]record, 0, len(rows))
			models := 0
			for _, r := range rows {
				records = append(records, r)
				if !gatewayCoreTarget(r.Target) {
					continue
				}
				if r.Kind == "ws_handshake" {
					require.Equal(t, "HTTP/1.1", r.Proto)
					continue
				}
				if r.Kind != "http_request" {
					continue
				}
				require.Equal(t, "HTTP/2.0", r.Proto)
				require.NotNil(t, r.H2)
				require.EqualValues(t, 1, r.H2.StreamID)
				require.Equal(t, 1, r.ConnReq)
				require.Equal(t, []h2Setting{{2, 0}, {4, 2097152}, {5, 16384}, {6, 16384}}, r.H2.Settings)
				require.EqualValues(t, 5177345, r.H2.ConnectionWindowIncrement)
				if strings.Contains(r.Target, "/codex/models?") {
					models++
				}
			}
			require.Equal(t, 2, models)
			sort.Slice(records, func(i, j int) bool { return records[i].Seq < records[j].Seq })
			sc := scenario{Name: name, RejectWebSocket: strings.HasPrefix(name, "http_"), ToolLoop: strings.Contains(name, "tool_loop") || name == "ws_compaction", UnauthorizedUntilRefresh: strings.Contains(name, "unauthorized"), Compaction: name == "ws_compaction"}
			require.NoError(t, validateScenario(sc, records))
		})
	}
}
