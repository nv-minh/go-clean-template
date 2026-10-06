package redisstore

import (
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker/v2"
)

// newBreaker returns a circuit breaker tuned for Redis calls on the request path.
// After 5 consecutive failures it opens for 10s and callers skip Redis entirely (fail fast),
// then it lets a few probes through to detect recovery.
func newBreaker[T any](name string) *gobreaker.CircuitBreaker[T] {
	return gobreaker.NewCircuitBreaker[T](gobreaker.Settings{
		Name:        name,
		MaxRequests: 3,
		Interval:    30 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 5 },
		// redis.Nil (a miss) is a healthy answer, not a failure.
		IsSuccessful: func(err error) bool { return err == nil || errors.Is(err, redis.Nil) },
	})
}

func isBreakerOpen(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}
