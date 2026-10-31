package service

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// intelligentTestIdentityKey is the gin context key carrying the per-run
// session/thread identity minted for an OpenAI OAuth connectivity test.
const intelligentTestIdentityKey = "intelligent_test_identity"

type intelligentTestIdentity struct {
	sessionID string
	threadID  string
}

// prepareIntelligentTestProtection reuses the persisted device identity of an
// OpenAI OAuth account and issues a fresh session/thread per run, then projects
// the converged fingerprint into the test payload's client_metadata. It never
// writes account extra. The gate mirrors the real forwarding path: it only acts
// for OAuth-like accounts whose fingerprint mode is not off.
func prepareIntelligentTestProtection(c *gin.Context, account *Account, payload map[string]any) error {
	if c == nil || c.Request == nil || account == nil {
		return nil
	}
	if !account.IsOpenAIOAuthLike() || account.GetCodexFingerprintMode() == codexFingerprintOff {
		return nil
	}
	rawSession := uuid.NewString()
	ids := resolveCodexFingerprintIDs(account, rawSession, account.GetCodexFingerprintMode())
	stageCodexFingerprintIDs(c, ids)
	identity := intelligentTestIdentity{sessionID: rawSession, threadID: uuid.NewString()}
	if ids != nil {
		if ids.sessionID != "" {
			identity.sessionID = ids.sessionID
		}
		if ids.threadID != "" {
			identity.threadID = ids.threadID
		}
	}
	c.Request.Header.Set("session-id", rawSession)
	c.Set(intelligentTestIdentityKey, identity)
	if payload != nil {
		applyStagedCodexFingerprintClientMetadata(c, account, payload)
		if metadata, ok := payload["client_metadata"].(map[string]any); ok {
			metadata["session_id"] = identity.sessionID
			metadata["thread_id"] = identity.threadID
		}
	}
	return nil
}

// applyIntelligentTestProtection projects the staged converged fingerprint onto
// the outbound headers and pins the conversation/thread identity, matching the
// official wire shape used by real Codex forwarding.
func applyIntelligentTestProtection(c *gin.Context, account *Account, headers http.Header, payload []byte) error {
	if c == nil {
		return nil
	}
	value, exists := c.Get(intelligentTestIdentityKey)
	identity, ok := value.(intelligentTestIdentity)
	if !exists || !ok {
		return nil
	}
	applyStagedCodexFingerprintHeaders(c, account, headers)
	applyCodexFingerprintConvergenceHeaders(c, codexAccountIdentitySource(c, account), headers)
	headers.Set("conversation_id", identity.sessionID)
	headers.Set("thread-id", identity.threadID)
	return nil
}
