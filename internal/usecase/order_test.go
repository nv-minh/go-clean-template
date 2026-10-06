package usecase_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/usecase"
)

var (
	t0       = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	customer = uuid.MustParse("0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
	items    = []domain.Item{{SKU: "A", Quantity: 2, UnitPrice: 100}}
)

type fakeRepo struct {
	mu        sync.Mutex
	orders    map[uuid.UUID]*domain.Order
	getCalls  atomic.Int32
	getDelay  time.Duration
	updateErr error
	listRows  []*domain.Order
}

func newFakeRepo() *fakeRepo { return &fakeRepo{orders: map[uuid.UUID]*domain.Order{}} }

func (r *fakeRepo) Create(_ context.Context, o *domain.Order) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.orders[o.ID] = o
	return nil
}

func (r *fakeRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.Order, error) {
	r.getCalls.Add(1)
	time.Sleep(r.getDelay)
	r.mu.Lock()
	defer r.mu.Unlock()
	o, ok := r.orders[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *o
	return &cp, nil
}

func (r *fakeRepo) List(_ context.Context, p domain.ListParams) ([]*domain.Order, error) {
	if len(r.listRows) > p.Limit {
		return r.listRows[:p.Limit], nil
	}
	return r.listRows, nil
}

func (r *fakeRepo) UpdateStatus(_ context.Context, o *domain.Order) error {
	if r.updateErr != nil {
		return r.updateErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	o.Version++
	cp := *o
	r.orders[o.ID] = &cp
	return nil
}

type fakeOutbox struct {
	events []domain.Event
	err    error
}

func (o *fakeOutbox) Append(_ context.Context, e domain.Event) error {
	if o.err != nil {
		return o.err
	}
	o.events = append(o.events, e)
	return nil
}

type fakeTx struct{}

func (fakeTx) WithinTx(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

type fakeCache struct {
	mu      sync.Mutex
	m       map[uuid.UUID]*domain.Order
	getErr  error
	deleted []uuid.UUID
}

func newFakeCache() *fakeCache { return &fakeCache{m: map[uuid.UUID]*domain.Order{}} }

func (c *fakeCache) Get(_ context.Context, id uuid.UUID) (*domain.Order, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, c.getErr
	}
	if o, ok := c.m[id]; ok {
		return o, nil
	}
	return nil, domain.ErrCacheMiss
}

func (c *fakeCache) Set(_ context.Context, o *domain.Order) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[o.ID] = o
	return nil
}

func (c *fakeCache) Delete(_ context.Context, id uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, id)
	c.deleted = append(c.deleted, id)
	return nil
}

type env struct {
	svc    *usecase.OrderService
	repo   *fakeRepo
	outbox *fakeOutbox
	cache  *fakeCache
}

func newEnv() env {
	e := env{repo: newFakeRepo(), outbox: &fakeOutbox{}, cache: newFakeCache()}
	e.svc = usecase.NewOrderService(e.repo, e.outbox, fakeTx{}, e.cache, slog.New(slog.DiscardHandler),
		usecase.WithClock(func() time.Time { return t0 }))
	return e
}

func TestCreateWritesOrderAndOutboxEvent(t *testing.T) {
	t.Parallel()
	e := newEnv()
	o, err := e.svc.Create(t.Context(), usecase.CreateOrderInput{CustomerID: customer, Currency: "usd", Items: items})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.repo.orders[o.ID]; !ok {
		t.Fatal("order not persisted")
	}
	if len(e.outbox.events) != 1 || e.outbox.events[0].Type != domain.EventOrderCreated || e.outbox.events[0].AggregateID != o.ID.String() {
		t.Fatalf("unexpected outbox events: %+v", e.outbox.events)
	}
}

func TestCreateFailsWhenOutboxFails(t *testing.T) {
	t.Parallel()
	e := newEnv()
	e.outbox.err = errors.New("db down")
	if _, err := e.svc.Create(t.Context(), usecase.CreateOrderInput{CustomerID: customer, Currency: "USD", Items: items}); err == nil {
		t.Fatal("expected error so the surrounding transaction rolls back")
	}
}

func TestCreateRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	e := newEnv()
	_, err := e.svc.Create(t.Context(), usecase.CreateOrderInput{CustomerID: customer, Currency: "USD"})
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
	if len(e.outbox.events) != 0 {
		t.Fatal("no event must be emitted for invalid input")
	}
}

func TestGetUsesCacheAndCollapsesConcurrentMisses(t *testing.T) {
	t.Parallel()
	e := newEnv()
	o, _ := e.svc.Create(t.Context(), usecase.CreateOrderInput{CustomerID: customer, Currency: "USD", Items: items})
	e.repo.getDelay = 50 * time.Millisecond

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if _, err := e.svc.Get(t.Context(), o.ID); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if n := e.repo.getCalls.Load(); n != 1 {
		t.Fatalf("20 concurrent misses must hit the database once, got %d", n)
	}
	if _, err := e.svc.Get(t.Context(), o.ID); err != nil || e.repo.getCalls.Load() != 1 {
		t.Fatal("subsequent read must be served from cache")
	}
}

func TestGetFallsBackToDatabaseWhenCacheFails(t *testing.T) {
	t.Parallel()
	e := newEnv()
	o, _ := e.svc.Create(t.Context(), usecase.CreateOrderInput{CustomerID: customer, Currency: "USD", Items: items})
	e.cache.getErr = errors.New("redis down")
	if _, err := e.svc.Get(t.Context(), o.ID); err != nil {
		t.Fatalf("cache failure must not fail the request: %v", err)
	}
}

func TestGetNotFound(t *testing.T) {
	t.Parallel()
	e := newEnv()
	if _, err := e.svc.Get(t.Context(), uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestCancel(t *testing.T) {
	t.Parallel()
	e := newEnv()
	o, _ := e.svc.Create(t.Context(), usecase.CreateOrderInput{CustomerID: customer, Currency: "USD", Items: items})

	got, err := e.svc.Cancel(t.Context(), o.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusCancelled || got.Version != 2 {
		t.Fatalf("unexpected order: %+v", got)
	}
	if len(e.outbox.events) != 2 || e.outbox.events[1].Type != domain.EventOrderCancelled {
		t.Fatalf("expected order.cancelled event, got %+v", e.outbox.events)
	}
	if len(e.cache.deleted) != 1 {
		t.Fatal("cache must be invalidated after commit")
	}
	if _, err := e.svc.Cancel(t.Context(), o.ID); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("second cancel must fail with ErrInvalidState, got %v", err)
	}
}

func TestCancelPropagatesConcurrencyConflict(t *testing.T) {
	t.Parallel()
	e := newEnv()
	o, _ := e.svc.Create(t.Context(), usecase.CreateOrderInput{CustomerID: customer, Currency: "USD", Items: items})
	e.repo.updateErr = domain.ErrConflict
	if _, err := e.svc.Cancel(t.Context(), o.ID); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	if len(e.cache.deleted) != 0 || len(e.outbox.events) != 1 {
		t.Fatal("a failed cancel must not invalidate the cache nor emit an event")
	}
}

func TestListPagination(t *testing.T) {
	t.Parallel()
	e := newEnv()
	for i := range 3 {
		e.repo.listRows = append(e.repo.listRows, &domain.Order{ID: uuid.New(), CreatedAt: t0.Add(-time.Duration(i) * time.Minute)})
	}
	res, err := e.svc.List(t.Context(), customer, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Orders) != 2 || res.Next == nil || res.Next.ID != res.Orders[1].ID {
		t.Fatalf("expected 2 rows and a cursor at the last row, got %d rows, next=%+v", len(res.Orders), res.Next)
	}
	res, err = e.svc.List(t.Context(), customer, nil, 10)
	if err != nil || len(res.Orders) != 3 || res.Next != nil {
		t.Fatalf("last page must have no cursor: rows=%d next=%+v err=%v", len(res.Orders), res.Next, err)
	}
	if _, err := e.svc.List(t.Context(), uuid.Nil, nil, 10); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("want ErrInvalid for missing customer, got %v", err)
	}
}
