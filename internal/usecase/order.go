// Package usecase contains the application business rules. It orchestrates domain entities
// through the ports declared in package domain and knows nothing about HTTP, SQL, Redis or Kafka.
package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"

	"github.com/yourorg/go-clean-template/internal/domain"
)

const (
	DefaultPageSize = 20
	MaxPageSize     = 100

	loadTimeout = 5 * time.Second
)

type OrderService struct {
	repo   domain.OrderRepository
	outbox domain.OutboxWriter
	tx     domain.TxManager
	cache  domain.OrderCache
	log    *slog.Logger
	now    func() time.Time
	loads  singleflight.Group
}

type Option func(*OrderService)

// WithClock overrides the time source (tests).
func WithClock(now func() time.Time) Option { return func(s *OrderService) { s.now = now } }

func NewOrderService(
	repo domain.OrderRepository,
	outbox domain.OutboxWriter,
	tx domain.TxManager,
	cache domain.OrderCache,
	log *slog.Logger,
	opts ...Option,
) *OrderService {
	s := &OrderService{
		repo: repo, outbox: outbox, tx: tx, cache: cache, log: log,
		now: func() time.Time { return time.Now().UTC() },
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

type CreateOrderInput struct {
	CustomerID uuid.UUID
	Currency   string
	Items      []domain.Item
}

// Create persists the order and its order.created event in ONE transaction.
func (s *OrderService) Create(ctx context.Context, in CreateOrderInput) (*domain.Order, error) {
	now := s.now()
	o, err := domain.NewOrder(in.CustomerID, in.Currency, in.Items, now)
	if err != nil {
		return nil, err
	}
	err = s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := s.repo.Create(ctx, o); err != nil {
			return err
		}
		return s.outbox.Append(ctx, domain.NewOrderEvent(domain.EventOrderCreated, o, now))
	})
	if err != nil {
		return nil, fmt.Errorf("create order: %w", err)
	}
	return o, nil
}

// Get is a read-through cache lookup. Concurrent misses for the same id are collapsed into a
// single database query (singleflight), which prevents a cache stampede on a hot key.
// Cache failures never fail the request: they degrade to a database read.
func (s *OrderService) Get(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	switch o, err := s.cache.Get(ctx, id); {
	case err == nil:
		return o, nil
	case !errors.Is(err, domain.ErrCacheMiss):
		s.log.WarnContext(ctx, "order cache get failed, falling back to database", slog.Any("error", err))
	}

	ch := s.loads.DoChan(id.String(), func() (any, error) {
		// Detach from the first caller's cancellation: the result is shared with other waiters.
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), loadTimeout)
		defer cancel()
		o, err := s.repo.GetByID(lctx, id)
		if err != nil {
			return nil, err
		}
		if err := s.cache.Set(lctx, o); err != nil {
			s.log.WarnContext(lctx, "order cache set failed", slog.Any("error", err))
		}
		return o, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		o, ok := r.Val.(*domain.Order)
		if !ok {
			return nil, fmt.Errorf("unexpected singleflight result %T", r.Val)
		}
		return o, nil
	}
}

type ListResult struct {
	Orders []*domain.Order
	Next   *domain.Cursor // nil when there are no more pages
}

// List returns a page of a customer's orders using keyset pagination: cost is O(page) no matter
// how deep the client paginates, unlike OFFSET which degrades linearly.
func (s *OrderService) List(ctx context.Context, customerID uuid.UUID, cursor *domain.Cursor, limit int) (*ListResult, error) {
	if customerID == uuid.Nil {
		return nil, fmt.Errorf("%w: customer_id is required", domain.ErrInvalid)
	}
	switch {
	case limit <= 0:
		limit = DefaultPageSize
	case limit > MaxPageSize:
		limit = MaxPageSize
	}
	// Fetch one extra row to learn whether another page exists without a COUNT query.
	rows, err := s.repo.List(ctx, domain.ListParams{CustomerID: customerID, Cursor: cursor, Limit: limit + 1})
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	res := &ListResult{Orders: rows}
	if len(rows) > limit {
		res.Orders = rows[:limit]
		last := res.Orders[limit-1]
		res.Next = &domain.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	return res, nil
}

// Cancel transitions the order and emits order.cancelled atomically, then invalidates the cache
// AFTER commit (invalidating before commit would let a concurrent reader re-cache the old value).
func (s *OrderService) Cancel(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	var o *domain.Order
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		var err error
		if o, err = s.repo.GetByID(ctx, id); err != nil {
			return err
		}
		now := s.now()
		if err := o.Cancel(now); err != nil {
			return err
		}
		if err := s.repo.UpdateStatus(ctx, o); err != nil {
			return err
		}
		return s.outbox.Append(ctx, domain.NewOrderEvent(domain.EventOrderCancelled, o, now))
	})
	if err != nil {
		return nil, fmt.Errorf("cancel order: %w", err)
	}
	if err := s.cache.Delete(ctx, id); err != nil {
		s.log.WarnContext(ctx, "order cache invalidation failed", slog.Any("error", err))
	}
	return o, nil
}
