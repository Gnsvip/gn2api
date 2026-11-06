package repository

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// 余额在途预留：
//   - billing:inflight:{uid}      ZSET  member=requestID score=过期时间(ms)
//   - billing:inflight_amt:{uid}  HASH  field=requestID value=预留金额(USD)
//
// Admission also reads billing:balance:<uid>. The existing standalone redis.Client
// supports this; the legacy balance key is not co-slotted for Redis Cluster.
const (
	billingInflightKeyPrefix    = "billing:inflight:"
	billingInflightAmtKeyPrefix = "billing:inflight_amt:"
)

func billingInflightKeys(userID int64) (string, string) {
	return fmt.Sprintf("%s{%d}", billingInflightKeyPrefix, userID),
		fmt.Sprintf("%s{%d}", billingInflightAmtKeyPrefix, userID)
}

// 订阅 / API Key 维度在途预留（确定金额费用）：
//   - billing:inflight_scope:{scope}      ZSET  member=requestID score=过期时间(ms)
//   - billing:inflight_scope_amt:{scope}  HASH  field=requestID value=预留金额(USD)
//
// 过期清理与释放语义与余额维度相同，不续期。
const (
	billingInflightScopeKeyPrefix    = "billing:inflight_scope:"
	billingInflightScopeAmtKeyPrefix = "billing:inflight_scope_amt:"
)

func billingInflightScopeKeys(scope string) (string, string) {
	return billingInflightScopeKeyPrefix + "{" + scope + "}", billingInflightScopeAmtKeyPrefix + "{" + scope + "}"
}

var (
	// KEYS: [1]=zset [2]=hash [3]=current balance
	// ARGV: [1]=now_ms [2]=expire_at_ms [3]=member [4]=amount [5]=minimum_balance [6]=key_ttl_ms
	// 返回 {allowed, inflight_sum(string), inflight_count}
	// A lone estimated request may exceed its starting balance, but the current
	// balance must be positive and meet minimum_balance at admission time.
	reserveInflightBalanceScript = redis.NewScript(`
		local raw = redis.call('GET', KEYS[3])
		if not raw then return {-1, '0', 0} end
		local balance = tonumber(raw)
		if not balance or balance ~= balance or balance == math.huge or balance == -math.huge then
			return redis.error_reply('invalid cached balance')
		end
		local now = tonumber(ARGV[1])
		local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now)
		if #expired > 0 then
			redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
			redis.call('HDEL', KEYS[2], unpack(expired))
		end
		local vals = redis.call('HVALS', KEYS[2])
		local sum = 0
		for i = 1, #vals do
			sum = sum + (tonumber(vals[i]) or 0)
		end
		local count = redis.call('ZCARD', KEYS[1])
		local amount = tonumber(ARGV[4])
		if balance <= 0 or balance < tonumber(ARGV[5]) then
			return {0, tostring(sum), count}
		end
		if count > 0 and (balance - sum) < amount then
			return {2, tostring(sum), count}
		end
		redis.call('ZADD', KEYS[1], tonumber(ARGV[2]), ARGV[3])
		redis.call('HSET', KEYS[2], ARGV[3], ARGV[4])
		redis.call('PEXPIRE', KEYS[1], ARGV[6])
		redis.call('PEXPIRE', KEYS[2], ARGV[6])
		return {1, tostring(sum), count}
	`)

	// KEYS: [1]=zset [2]=hash  ARGV: [1]=member [2]=expire_at_ms [3]=key_ttl_ms [4]=now_ms
	// 仅当成员仍存在且尚未过期（score > now）时续期（XX），并把两个 key 的 TTL 至少延长到 key_ttl_ms。
	// 已过期但尚未被惰性清理的成员不得被复活：顺手清掉并返回 0。
	renewInflightBalanceScript = redis.NewScript(`
		local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
		if not score then
			return 0
		end
		if tonumber(score) <= tonumber(ARGV[4]) then
			redis.call('ZREM', KEYS[1], ARGV[1])
			redis.call('HDEL', KEYS[2], ARGV[1])
			return 0
		end
		redis.call('ZADD', KEYS[1], 'XX', tonumber(ARGV[2]), ARGV[1])
		local ttl = tonumber(ARGV[3])
		if redis.call('PTTL', KEYS[1]) < ttl then
			redis.call('PEXPIRE', KEYS[1], ttl)
		end
		if redis.call('PTTL', KEYS[2]) < ttl then
			redis.call('PEXPIRE', KEYS[2], ttl)
		end
		return 1
	`)

	releaseInflightBalanceScript = redis.NewScript(`
		redis.call('ZREM', KEYS[1], ARGV[1])
		redis.call('HDEL', KEYS[2], ARGV[1])
		return 1
	`)

	invalidateAndReleaseInflightBalanceScript = redis.NewScript(`
		redis.call('SET', KEYS[4], ARGV[2], 'PX', ARGV[3])
		redis.call('DEL', KEYS[3])
		redis.call('ZREM', KEYS[1], ARGV[1])
		redis.call('HDEL', KEYS[2], ARGV[1])
		return 1
	`)

	// KEYS: [1]=zset [2]=hash
	// ARGV: [1]=now_ms [2]=expire_at_ms [3]=member [4]=amount [5]=key_ttl_ms
	// 先清理已过期成员，再登记本次预留；返回登记前其他在途预留的合计（string），是否放行由调用方按权威用量判定。
	reserveInflightScopeScript = redis.NewScript(`
		local now = tonumber(ARGV[1])
		local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now)
		if #expired > 0 then
			redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
			redis.call('HDEL', KEYS[2], unpack(expired))
		end
		local vals = redis.call('HVALS', KEYS[2])
		local sum = 0
		for i = 1, #vals do
			sum = sum + (tonumber(vals[i]) or 0)
		end
		redis.call('ZADD', KEYS[1], tonumber(ARGV[2]), ARGV[3])
		redis.call('HSET', KEYS[2], ARGV[3], ARGV[4])
		redis.call('PEXPIRE', KEYS[1], ARGV[5])
		redis.call('PEXPIRE', KEYS[2], ARGV[5])
		return tostring(sum)
	`)
)

// ReserveInflightBalance 实现 service.InflightBalanceReservationCache。
func (c *billingCache) ReserveInflightBalance(ctx context.Context, userID int64, requestID string, amount, minimumBalance float64, ttl time.Duration) (bool, float64, error) {
	if amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) || minimumBalance < 0 || math.IsNaN(minimumBalance) || math.IsInf(minimumBalance, 0) {
		return false, 0, fmt.Errorf("invalid balance reservation amount or minimum")
	}
	zkey, hkey := billingInflightKeys(userID)
	now := time.Now().UnixMilli()
	ttlMs := ttl.Milliseconds()
	if ttlMs <= 0 {
		ttlMs = 1
	}
	res, err := reserveInflightBalanceScript.Run(ctx, c.rdb, []string{zkey, hkey, billingBalanceKey(userID)},
		now,
		now+ttlMs,
		requestID,
		strconv.FormatFloat(amount, 'f', -1, 64),
		strconv.FormatFloat(minimumBalance, 'f', -1, 64),
		ttlMs,
	).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(res) < 2 {
		return false, 0, fmt.Errorf("unexpected inflight reservation reply: %v", res)
	}
	allowed, _ := res[0].(int64)
	if allowed == -1 {
		return false, 0, service.ErrBalanceCacheMiss
	}
	var sum float64
	if s, ok := res[1].(string); ok {
		sum, _ = strconv.ParseFloat(s, 64)
	}
	if allowed == 2 {
		return false, sum, service.ErrBalanceTemporarilyReserved
	}
	return allowed == 1, sum, nil
}

func (c *billingCache) InvalidateAndReleaseInflightBalance(ctx context.Context, userID int64, requestID string) error {
	zkey, hkey := billingInflightKeys(userID)
	return invalidateAndReleaseInflightBalanceScript.Run(ctx, c.rdb,
		[]string{zkey, hkey, billingBalanceKey(userID), billingBalanceVersionKey(userID)},
		requestID, uuid.NewString(), billingBalanceVersionTTL.Milliseconds()).Err()
}

// ReleaseInflightBalance 实现 service.InflightBalanceReservationCache。
func (c *billingCache) ReleaseInflightBalance(ctx context.Context, userID int64, requestID string) error {
	zkey, hkey := billingInflightKeys(userID)
	return releaseInflightBalanceScript.Run(ctx, c.rdb, []string{zkey, hkey}, requestID).Err()
}

// RenewInflightBalance 实现 service.InflightBalanceReservationRenewer。
func (c *billingCache) RenewInflightBalance(ctx context.Context, userID int64, requestID string, ttl time.Duration) (bool, error) {
	zkey, hkey := billingInflightKeys(userID)
	ttlMs := ttl.Milliseconds()
	if ttlMs <= 0 {
		ttlMs = 1
	}
	now := time.Now().UnixMilli()
	n, err := renewInflightBalanceScript.Run(ctx, c.rdb, []string{zkey, hkey}, requestID, now+ttlMs, ttlMs, now).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// ReserveInflightScope 实现 service.InflightScopeReservationCache。
func (c *billingCache) ReserveInflightScope(ctx context.Context, scope, requestID string, amount float64, ttl time.Duration) (float64, error) {
	zkey, hkey := billingInflightScopeKeys(scope)
	now := time.Now().UnixMilli()
	ttlMs := ttl.Milliseconds()
	if ttlMs <= 0 {
		ttlMs = 1
	}
	res, err := reserveInflightScopeScript.Run(ctx, c.rdb, []string{zkey, hkey},
		now,
		now+ttlMs,
		requestID,
		strconv.FormatFloat(amount, 'f', -1, 64),
		ttlMs,
	).Text()
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(res, 64)
}

// ReleaseInflightScope 实现 service.InflightScopeReservationCache。
func (c *billingCache) ReleaseInflightScope(ctx context.Context, scope, requestID string) error {
	zkey, hkey := billingInflightScopeKeys(scope)
	return releaseInflightBalanceScript.Run(ctx, c.rdb, []string{zkey, hkey}, requestID).Err()
}

var (
	_ service.InflightBalanceReservationCache   = (*billingCache)(nil)
	_ service.InflightBalanceReservationRenewer = (*billingCache)(nil)
	_ service.InflightScopeReservationCache     = (*billingCache)(nil)
)
