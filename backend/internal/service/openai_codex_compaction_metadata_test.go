package service

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCodexOAuthAstraContextFallback(t *testing.T) {
	for _, tc := range []struct {
		name, kind, metadata string
		window, max          int64
	}{
		{"oauth absent", AccountTypeOAuth, ``, 272000, 872000},
		{"oauth other metadata", AccountTypeOAuth, `"reasoning":true`, 272000, 872000},
		{"oauth explicit larger", AccountTypeOAuth, `"context_window":500000,"max_context_window":1000000`, 500000, 1000000},
		{"api absent", AccountTypeAPIKey, ``, 1050000, 1050000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := Account{ID: 1, Platform: PlatformOpenAI, Type: tc.kind, Credentials: map[string]any{"model_mapping": map[string]any{"gpt-6-astra": "gpt-6-astra"}}}
			if tc.metadata != "" {
				a = codexContextWindowAccount(t, 1, tc.metadata)
				a.Type = tc.kind
			}
			model := codexContextWindowManifest(t, []Account{a})
			require.EqualValues(t, tc.window, model["context_window"])
			require.EqualValues(t, tc.max, model["max_context_window"])
			require.Nil(t, model["auto_compact_token_limit"], "do not invent an early compaction threshold")
		})
	}
}

func TestCodexCompactionMetadataSurvivesSync(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		limit        any
		percent      int64
	}{
		{"explicit", `"auto_compact_token_limit":240000,"effective_context_window_percent":90`, float64(240000), 90},
		{"defaults", `"auto_compact_token_limit":null`, nil, 95},
		{"invalid", `"auto_compact_token_limit":-1,"effective_context_window_percent":120`, nil, 95},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"models":[{"slug":"gpt-6-astra","context_window":272000,"max_context_window":872000,` + tc.fields + `}]}`)
			_, metadata, err := extractUpstreamModelCatalog(body, false)
			require.NoError(t, err)
			a := Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
			a.SetUpstreamModelMetadataSnapshot(UpstreamModelMetadataSnapshot{Source: "upstream", Models: metadata})
			stored, err := json.Marshal(a.Extra)
			require.NoError(t, err)
			a.Extra = nil
			require.NoError(t, json.Unmarshal(stored, &a.Extra))
			model := codexContextWindowManifest(t, []Account{a})
			require.Equal(t, tc.limit, model["auto_compact_token_limit"])
			require.EqualValues(t, tc.percent, model["effective_context_window_percent"])
			require.EqualValues(t, 272000, model["context_window"])
			require.EqualValues(t, 872000, model["max_context_window"])
		})
	}
}

func TestCodexCompactionLiveNullOverridesSyncedThreshold(t *testing.T) {
	a := codexContextWindowAccount(t, 1, `"context_window":272000,"auto_compact_token_limit":240000,"effective_context_window_percent":90`)
	body, err := applySyncedAPIKeyCodexModelMetadata([]byte(`{"models":[{"slug":"gpt-6-astra","auto_compact_token_limit":null,"effective_context_window_percent":95}]}`), &a, false)
	require.NoError(t, err)
	model := decodeCodexManifestModels(t, body)[0]
	require.Nil(t, model["auto_compact_token_limit"])
	require.EqualValues(t, 95, model["effective_context_window_percent"])
}
