// Package redisx creates the Redis client.
package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yourorg/go-clean-template/internal/platform/config"
)

// New returns a client with aggressive timeouts. Redis is used for cache, rate limiting,
// idempotency and realtime fan-out, all of which must fail fast and degrade gracefully
// instead of stalling request goroutines.
func New(ctx context.Context, cfg config.Redis) (*redis.Client, error) {
	c := redis.NewClient(&redis.Options{
		Addr:            cfg.Addr,
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    2,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		PoolTimeout:     cfg.ReadTimeout + 100*time.Millisecond,
		MaxRetries:      1,
		MinRetryBackoff: 5 * time.Millisecond,
		MaxRetryBackoff: 50 * time.Millisecond,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Ping(pingCtx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return c, nil
}
