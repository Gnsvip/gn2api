package service

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCodexProtocolScopeExcludesForeignOAuth(t *testing.T) {
	for _, platform := range []string{PlatformGrok, PlatformAnthropic, PlatformGemini, PlatformAntigravity} {
		a := &Account{Platform: platform, Type: AccountTypeOAuth,
			Extra: map[string]any{codexTimezoneExtraKey: "America/Chicago"}}
		require.False(t, a.UsesOpenAICodexProtocol(), platform)
		require.False(t, isCodexOAuthAccount(a), platform)
		require.Empty(t, codexAccountTimezone(a), platform)
	}
	for _, platform := range []string{"", PlatformOpenAI} {
		require.True(t, (&Account{Platform: platform, Type: AccountTypeOAuth}).UsesOpenAICodexProtocol())
	}
}
