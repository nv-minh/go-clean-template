// Package redisstore holds every Redis backed adapter: order cache, rate limiter, idempotency
// store and realtime (pub/sub) hub.
package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker/v2"

	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/platform/metrics"
)

// OrderCache is a cache-aside store with two protections against a sick Redis:
//   - a circuit breaker, so once Redis is failing we stop paying its timeout on every request and
//     go straight to the database;
//   - TTL jitter, so keys written together do not all expire (and hit the database) together.
type OrderCache struct {
	rdb     *redis.Client
	ttl     time.Duration
	breaker *gobreaker.CircuitBreaker[[]byte]
	metrics *metrics.Metrics
}

var _ domain.OrderCache = (*OrderCache)(nil)

func NewOrderCache(rdb *redis.Client, ttl time.Duration, m *metrics.Metrics) *OrderCache {
	return &OrderCache{
		rdb: rdb, ttl: ttl, metrics: m,
		breaker: newBreaker[[]byte]("redis-cache"),
	}
}

// cachedOrder is the stable wire format stored in Redis (the domain type carries no tags).
type cachedOrder struct {
	ID          uuid.UUID     `json:"id"`
	CustomerID  uuid.UUID     `json:"customer_id"`
	Status      domain.Status `json:"status"`
	Currency    string        `json:"currency"`
	TotalAmount int64         `json:"total_amount"`
	Items       []domain.Item `json:"items"`
	Version     int32         `json:"version"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

func cacheKey(id uuid.UUID) string { return "order:v1:" + id.String() }

func (c *OrderCache) Get(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	raw, err := c.breaker.Execute(func() ([]byte, error) { return c.rdb.Get(ctx, cacheKey(id)).Bytes() })
	switch {
	case errors.Is(err, redis.Nil):
		c.metrics.CacheOps.WithLabelValues("miss").Inc()
		return nil, domain.ErrCacheMiss
	case isBreakerOpen(err):
		c.metrics.CacheOps.WithLabelValues("skipped").Inc()
		return nil, domain.ErrCacheMiss
	case err != nil:
		c.metrics.CacheOps.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("redis get: %w", err)
	}

	var co cachedOrder
	if err := json.Unmarshal(raw, &co); err != nil {
		c.metrics.CacheOps.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("decode cached order: %w", err)
	}
	c.metrics.CacheOps.WithLabelValues("hit").Inc()
	return &domain.Order{
		ID: co.ID, CustomerID: co.CustomerID, Status: co.Status, Currency: co.Currency,
		TotalAmount: co.TotalAmount, Items: co.Items, Version: co.Version,
		CreatedAt: co.CreatedAt, UpdatedAt: co.UpdatedAt,
	}, nil
}

func (c *OrderCache) Set(ctx context.Context, o *domain.Order) error {
	raw, err := json.Marshal(cachedOrder{
		ID: o.ID, CustomerID: o.CustomerID, Status: o.Status, Currency: o.Currency,
		TotalAmount: o.TotalAmount, Items: o.Items, Version: o.Version,
		CreatedAt: o.CreatedAt, UpdatedAt: o.UpdatedAt,
	})
	if err != nil {
		return fmt.Errorf("encode order for cache: %w", err)
	}
	ttl := c.ttl + time.Duration(rand.Int64N(int64(c.ttl/10)+1)) //nolint:gosec // jitter, not security
	_, err = c.breaker.Execute(func() ([]byte, error) { return nil, c.rdb.Set(ctx, cacheKey(o.ID), raw, ttl).Err() })
	return skipOpen(err)
}

func (c *OrderCache) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := c.breaker.Execute(func() ([]byte, error) { return nil, c.rdb.Del(ctx, cacheKey(id)).Err() })
	return skipOpen(err)
}

// skipOpen hides "breaker is open" from callers: it is expected during a Redis incident and the
// breaker state is already visible through metrics.
func skipOpen(err error) error {
	if isBreakerOpen(err) {
		return nil
	}
	return err
}
