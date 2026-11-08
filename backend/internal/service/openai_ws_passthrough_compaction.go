package service

import (
	"context"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// A recovery restarts only the upstream relay. The downstream reader, admitted
// turn, billing hooks and timing recorder survive until that turn completes.
type openAIPassthroughCompactionRetry struct {
	retry         *openAICompactionReplayRetry
	parentMissing bool
	completed     int
	model         string
	recorder      *requesttiming.Recorder
}

type openAIPassthroughRouteRestart struct {
	body      []byte
	completed int
	model     string
	started   time.Time
}

func (*openAIPassthroughRouteRestart) Error() string {
	return "route restored websocket request before upstream write"
}

type openAIWSRecoveryResumeKey struct{}
type openAIWSRecoveryResume struct {
	previous *openAICompactionRecoveryTurn
	offset   int
}

func (*openAIPassthroughCompactionRetry) Error() string {
	return "recover current websocket compaction context"
}

func (s *OpenAIGatewayService) proxyResponsesWebSocketV2Passthrough(ctx context.Context, c *gin.Context, client *coderws.Conn, account *Account, token string, first []byte, hooks *OpenAIWSIngressHooks, decision OpenAIWSProtocolDecision) error {
	if s == nil {
		return errors.New("service is nil")
	}
	ctx = WithOpenAIWSClientReadHandoff(ctx, client)
	defer finishCompactionRecoveryTurn(c)
	var previous *openAICompactionRecoveryTurn
	var recorder *requesttiming.Recorder
	offset, retries := 0, 0
	model := ""
	started := time.Time{}
	replay := &openAIWSPassthroughRecovery{limit: ResolveOpenAIWSClientReadLimitBytes(s.cfg)}
	replay.parents = openAIWSClientHistory(c, int(replay.limit))
	for {
		attemptHooks := offsetCompactionHooks(hooks, offset, model, started)
		if previous != nil && s.shouldBridgeOpenAIWSPassthroughFirstMessage(account, first) {
			resume := &openAIWSRecoveryResume{previous: previous, offset: offset}
			return s.ProxyResponsesWebSocketFromClient(context.WithValue(ctx, openAIWSRecoveryResumeKey{}, resume), c, client, account, token, first, attemptHooks)
		}
		err := s.proxyResponsesWebSocketV2PassthroughOnce(ctx, c, client, account, token, first, attemptHooks, decision, previous, recorder, offset, replay)
		var route *openAIPassthroughRouteRestart
		if errors.As(err, &route) {
			if controlErr := pendingWSCompactionControl(ctx); controlErr != nil {
				return controlErr
			}
			// No upstream or downstream event belongs to this new create yet.
			// Re-enter routing with the full window, preserving the reader and turn number.
			routeHooks := offsetCompactionHooks(hooks, offset+route.completed, route.model, route.started)
			return s.ProxyResponsesWebSocketFromClient(ctx, c, client, account, token, route.body, routeHooks)
		}
		var recovery *openAIPassthroughCompactionRetry
		if !errors.As(err, &recovery) {
			var failover *UpstreamFailoverError
			var current *openAIWSCurrentTurnFailoverError
			if previous != nil && errors.As(err, &failover) && !errors.As(err, &current) {
				if controlErr := pendingWSCompactionControl(ctx); controlErr != nil {
					if hooks != nil && hooks.AfterTurn != nil {
						hooks.AfterTurn(offset+1, nil, controlErr)
					}
					requesttiming.FinishRecorder(c, recorder, 499)
					return controlErr
				}
				// A fresh-socket handshake may fail before the relay starts.
				// The scheduler must retry this restored turn, not session turn 1.
				return newOpenAIWSCurrentTurnFailoverError(err, first)
			}
			return err
		}
		if recovery.completed > 0 {
			retries = 0
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		previous = compactionRecoveryTurn(c)
		if retries >= 2 {
			err = errors.New("automatic compaction recovery retry limit reached")
		} else {
			first, err = s.prepareWSCompactionRetry(ctx, c, account, recovery.retry)
		}
		if err != nil {
			var interrupted *OpenAIWSClientCloseError
			if errors.As(err, &interrupted) || ctx.Err() != nil {
				if hooks != nil && hooks.AfterTurn != nil {
					hooks.AfterTurn(offset+recovery.completed+1, nil, err)
				}
				requesttiming.FinishRecorder(c, recovery.recorder, 499)
				return err
			}
			if failover := compactionAccountFailover(ctx, c, recovery.retry, err); failover != nil {
				if hooks != nil && hooks.AfterTurn != nil {
					hooks.AfterTurn(offset+recovery.completed+1, nil, failover)
				}
				return newOpenAIWSCurrentTurnFailoverError(failover, RemovePreviousResponseIDFromBody(recovery.retry.body))
			}
			writeCtx, cancel := newOpenAIWSDownstreamWriteContext(ctx, hooks, s.openAIWSWriteTimeout())
			_ = client.Write(writeCtx, coderws.MessageText, openAIRecompactionErrorEvent(err))
			cancel()
			if hooks != nil && hooks.AfterTurn != nil {
				hooks.AfterTurn(offset+recovery.completed+1, nil, err)
			}
			requesttiming.FinishRecorder(c, recovery.recorder, 400)
			return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "automatic context recovery failed", errors.New("automatic context recovery failed"))
		}
		offset += recovery.completed
		retries++
		model, recorder = recovery.model, recovery.recorder
		replay.upstreamReconnected()
		if c != nil && c.Request != nil {
			c.Request.Header.Del(openAIWSTurnStateHeader)
		}
		if previous != nil {
			started = previous.started
		}
	}
}

func offsetCompactionHooks(hooks *OpenAIWSIngressHooks, offset int, model string, started time.Time) *OpenAIWSIngressHooks {
	result := &OpenAIWSIngressHooks{}
	if hooks != nil {
		*result = *hooks
	}
	if model != "" {
		result.InitialRequestModel = model
	}
	if !started.IsZero() {
		result.InitialTurnStartedAt = started
	}
	if hooks == nil || offset == 0 {
		return result
	}
	if hooks.BeforeTurn != nil {
		result.BeforeTurn = func(n int) error { return hooks.BeforeTurn(offset + n) }
	}
	if hooks.BeforeRequest != nil {
		result.BeforeRequest = func(n int, body []byte, model string) error { return hooks.BeforeRequest(offset+n, body, model) }
	}
	if hooks.MapRequestModel != nil {
		result.MapRequestModel = func(n int, model string) (string, error) { return hooks.MapRequestModel(offset+n, model) }
	}
	if hooks.TurnStarted != nil {
		result.TurnStarted = func(n int, start time.Time) { hooks.TurnStarted(offset+n, start) }
	}
	if hooks.AfterTurn != nil {
		result.AfterTurn = func(n int, value *OpenAIForwardResult, err error) { hooks.AfterTurn(offset+n, value, err) }
	}
	return result
}

func (s *OpenAIGatewayService) beginPassthroughCompaction(c *gin.Context, replay *openAIWSPassthroughRecovery, body []byte, model string, account *Account, started time.Time) []byte {
	if model != "" && gjson.GetBytes(body, "model").String() == "" {
		body = s.ReplaceModelInBody(body, model)
	}
	replay.mu.Lock()
	replay.selectParent(gjson.GetBytes(body, "previous_response_id").String())
	history, id, complete := replay.history, replay.lastResponseID, replay.historyComplete && !replay.incompatible
	stale := replay.historyDetached
	incompatible := replay.incompatible
	replay.mu.Unlock()
	if incompatible {
		// A prior untracked edit also forbids substituting an older undo on
		// ingress. Continue forwarding this session without reconstructing it.
		if c != nil {
			c.Set(openAICompactionRecoveryContextKey, nil)
		}
		replay.begin(body, model)
		return body
	}
	if stale && complete && int64(len(body)) <= replay.limit {
		// An old socket's ID cannot carry the context to its replacement. Only
		// replace it when this exact branch has complete, portable input.
		items, exists, err := buildOpenAIWSReplayInputSequence(history, true, body, true)
		if err == nil && exists {
			// Count the merged input once: some clients resend the full prefix
			// even with a parent ID. Do not reject that prefix as double-sized.
			size := int64(len(body)-len(gjson.GetBytes(body, "input").Raw)) + 2
			for _, item := range items {
				size += int64(len(item)) + 1
			}
			if size <= replay.limit && openAIWSReplayInputPortable(items) {
				if full, safe, err := buildOpenAIWSCurrentTurnRetryPayload(body, items, true, model); err == nil && safe && int64(len(full)) <= replay.limit {
					body = full
				}
			}
		}
	}
	body = s.beginCompactionRecoveryWithReplay(c, body, history, id, complete, account)
	compactionRecoveryStartTime(c, started)
	replay.begin(body, model)
	if !replay.canRetry() {
		if turn := compactionRecoveryTurn(c); turn != nil {
			turn.source, turn.output = nil, nil
		}
	}
	return body
}
