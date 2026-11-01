//go:build comparison_harness

package main

import (
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAcceptanceTargetMustBeExplicitAndMatchCredentialsOwner(t *testing.T) {
	id := int64(11)
	a := &service.Account{ID: 7001, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Credentials: map[string]any{"access_token": "synthetic"}, ProxyID: &id, Proxy: &service.Proxy{ID: id}}
	require.NoError(t, validateTarget(a, 7001))
	require.Error(t, validateTarget(a, 0))
	require.Error(t, validateTarget(a, 7002))
	a.Proxy.ID = 3
	require.Error(t, validateTarget(a, 7001))
	a.Proxy.ID = id
	a.Credentials["access_token"] = ""
	require.Error(t, validateTarget(a, 7001))
}

func TestAcceptanceStreamRetainsTerminalOnceWithoutDeltas(t *testing.T) {
	raw := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"status\":\"completed\"}}\n\ndata: [DONE]\n\n"
	kinds := []string{}
	body := &auditBody{ReadCloser: io.NopCloser(iotest.OneByteReader(strings.NewReader(raw))), record: func(kind string, _ any) { kinds = append(kinds, kind) }}
	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, raw, string(got))
	require.NoError(t, body.Close())
	require.NoError(t, body.Close())
	require.Equal(t, []string{"upstream_terminal", "upstream_stream_summary"}, kinds)
	require.Equal(t, int64(len(raw)), body.bytesRead)
}
