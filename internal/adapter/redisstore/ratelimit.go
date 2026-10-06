package redisstore

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker/v2"
)

// tokenBucket runs atomically inside Redis, so the limit is shared by every API replica.
// Time comes from Redis (TIME), not from the callers, which removes clock skew between pods.
// KEYS[1] bucket key; ARGV[1] refill rate (tokens/s); ARGV[2] burst capacity.
// Returns {allowed (0/1), retry_after_ms}.
var tokenBucket = redis.NewScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end

tokens = math.min(burst, tokens + math.max(0, now - ts) / 1000 * rate)
local allowed = 0
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.ceil((1 - tokens) / rate * 1000)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, retry}
`)

// RateLimiter is a distributed token bucket: rps sustained, bursts up to burst.
type RateLimiter struct {
	rdb     *redis.Client
	rps     float64
	burst   int
	breaker *gobreaker.CircuitBreaker[[]int64]
}

func NewRateLimiter(rdb *redis.Client, rps float64, burst int) *RateLimiter {
	return &RateLimiter{rdb: rdb, rps: rps, burst: burst, breaker: newBreaker[[]int64]("redis-ratelimit")}
}

// Allow consumes one token for key. When denied it returns how long to wait before retrying.
// While the circuit breaker is open (Redis unhealthy) requests are allowed without touching
// Redis: fail open, and no per-request timeout penalty during the incident.
func (l *RateLimiter) Allow(ctx context.Context, key string) (bool, time.Duration, error) {
	res, err := l.breaker.Execute(func() ([]int64, error) {
		return tokenBucket.Run(ctx, l.rdb, []string{"rl:" + key}, l.rps, l.burst).Int64Slice()
	})
	if isBreakerOpen(err) {
		return true, 0, nil
	}
	if err != nil {
		return false, 0, fmt.Errorf("rate limit script: %w", err)
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("rate limit script: unexpected reply %v", res)
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond, nil
}
