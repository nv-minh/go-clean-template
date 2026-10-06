//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/yourorg/go-clean-template/internal/adapter/repository"
	"github.com/yourorg/go-clean-template/internal/domain"
)

func newOrder(t *testing.T, customer uuid.UUID, at time.Time) *domain.Order {
	t.Helper()
	o, err := domain.NewOrder(customer, "USD", []domain.Item{{SKU: "A", Quantity: 2, UnitPrice: 100}}, at)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestOrderRepositoryRoundTripAndKeysetPagination(t *testing.T) {
	store := repository.NewStore(pool)
	repo := repository.NewOrderRepository(store)
	customer := uuid.New()
	base := time.Now().UTC().Truncate(time.Microsecond)

	var ids []uuid.UUID
	for i := range 5 {
		o := newOrder(t, customer, base.Add(time.Duration(i)*time.Second))
		if err := repo.Create(t.Context(), o); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, o.ID)
	}

	got, err := repo.GetByID(t.Context(), ids[0])
	if err != nil || got.TotalAmount != 200 || got.Items[0].SKU != "A" || got.Version != 1 {
		t.Fatalf("round trip failed: %+v err=%v", got, err)
	}
	if _, err := repo.GetByID(t.Context(), uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	// Walk all pages of size 2: newest first, every row exactly once, no gaps.
	var seen []uuid.UUID
	var cursor *domain.Cursor
	for range 10 {
		page, err := repo.List(t.Context(), domain.ListParams{CustomerID: customer, Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page {
			seen = append(seen, o.ID)
		}
		if len(page) < 2 {
			break
		}
		last := page[len(page)-1]
		cursor = &domain.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if len(seen) != 5 {
		t.Fatalf("want 5 rows across pages, got %d", len(seen))
	}
	for i, id := range seen {
		if id != ids[len(ids)-1-i] {
			t.Fatalf("row %d out of order", i)
		}
	}
}

func TestOptimisticConcurrencyControl(t *testing.T) {
	repo := repository.NewOrderRepository(repository.NewStore(pool))
	o := newOrder(t, uuid.New(), time.Now().UTC())
	if err := repo.Create(t.Context(), o); err != nil {
		t.Fatal(err)
	}

	a, _ := repo.GetByID(t.Context(), o.ID)
	b, _ := repo.GetByID(t.Context(), o.ID)
	if err := a.Cancel(time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateStatus(t.Context(), a); err != nil || a.Version != 2 {
		t.Fatalf("first writer must win: err=%v version=%d", err, a.Version)
	}
	if err := b.Cancel(time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateStatus(t.Context(), b); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale writer must get ErrConflict, got %v", err)
	}
}

func TestOutboxIsAtomicWithBusinessWrite(t *testing.T) {
	store := repository.NewStore(pool)
	repo := repository.NewOrderRepository(store)
	outbox := repository.NewOutbox(store)

	failing := newOrder(t, uuid.New(), time.Now().UTC())
	errBoom := errors.New("boom")
	err := store.WithinTx(t.Context(), func(ctx context.Context) error {
		if err := repo.Create(ctx, failing); err != nil {
			return err
		}
		if err := outbox.Append(ctx, domain.NewOrderEvent(domain.EventOrderCreated, failing, time.Now())); err != nil {
			return err
		}
		return errBoom // roll everything back
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("want boom, got %v", err)
	}
	if _, err := repo.GetByID(t.Context(), failing.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("rolled back order must not exist")
	}
	var n int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, failing.ID.String()).Scan(&n); err != nil || n != 0 {
		t.Fatalf("rolled back event must not exist (n=%d err=%v)", n, err)
	}

	ok := newOrder(t, uuid.New(), time.Now().UTC())
	if err := store.WithinTx(t.Context(), func(ctx context.Context) error {
		if err := repo.Create(ctx, ok); err != nil {
			return err
		}
		return outbox.Append(ctx, domain.NewOrderEvent(domain.EventOrderCreated, ok, time.Now()))
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, ok.ID.String()).Scan(&n); err != nil || n != 1 {
		t.Fatalf("committed event must exist (n=%d err=%v)", n, err)
	}
}

func TestOutboxBatchProcessing(t *testing.T) {
	store := repository.NewStore(pool)
	outbox := repository.NewOutbox(store)
	if _, err := pool.Exec(t.Context(), `TRUNCATE outbox`); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		e := domain.Event{Type: "t", AggregateType: "a", AggregateID: "agg", Payload: map[string]int{"i": i}}
		if err := outbox.Append(t.Context(), e); err != nil {
			t.Fatal(err)
		}
	}

	// A failing callback must leave every event unpublished.
	if _, err := outbox.ProcessBatch(t.Context(), 10, func(context.Context, []domain.OutboxEvent) error { return errors.New("kafka down") }); err == nil {
		t.Fatal("expected error")
	}

	// While one relay holds the batch, a second relay must back off (advisory lock).
	entered, release := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	var first int
	wg.Go(func() {
		first, _ = outbox.ProcessBatch(t.Context(), 3, func(_ context.Context, ev []domain.OutboxEvent) error {
			if len(ev) != 3 || ev[0].ID >= ev[1].ID || ev[1].ID >= ev[2].ID {
				t.Errorf("batch must be 3 events in id order: %+v", ev)
			}
			close(entered)
			<-release
			return nil
		})
	})
	<-entered
	n, err := outbox.ProcessBatch(t.Context(), 10, func(context.Context, []domain.OutboxEvent) error {
		t.Error("second relay must not process while the first holds the lock")
		return nil
	})
	if err != nil || n != 0 {
		t.Fatalf("second relay: n=%d err=%v", n, err)
	}
	close(release)
	wg.Wait()
	if first != 3 {
		t.Fatalf("first relay processed %d, want 3", first)
	}

	n, err = outbox.ProcessBatch(t.Context(), 10, func(_ context.Context, ev []domain.OutboxEvent) error {
		if len(ev) != 2 {
			t.Errorf("want the 2 remaining events, got %d", len(ev))
		}
		return nil
	})
	if err != nil || n != 2 {
		t.Fatalf("remaining: n=%d err=%v", n, err)
	}
	deleted, err := outbox.DeletePublishedBefore(t.Context(), time.Now().Add(time.Minute))
	if err != nil || deleted != 5 {
		t.Fatalf("cleanup deleted %d, want 5 (err=%v)", deleted, err)
	}
}

func TestProcessedEventsClaim(t *testing.T) {
	store := repository.NewStore(pool)
	claims := repository.NewProcessedEvents(store)
	id := uuid.NewString()

	if ok, err := claims.Claim(t.Context(), "c1", id); err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	if ok, _ := claims.Claim(t.Context(), "c1", id); ok {
		t.Fatal("second claim of the same event must be rejected")
	}
	if ok, _ := claims.Claim(t.Context(), "c2", id); !ok {
		t.Fatal("another consumer group must be able to claim the same event")
	}

	// A claim made in a transaction that rolls back is released, so the retry can process again.
	rb := uuid.NewString()
	_ = store.WithinTx(t.Context(), func(ctx context.Context) error {
		if ok, _ := claims.Claim(ctx, "c1", rb); !ok {
			t.Error("claim inside tx must succeed")
		}
		return errors.New("handler failed")
	})
	if ok, _ := claims.Claim(t.Context(), "c1", rb); !ok {
		t.Fatal("claim must be released when the transaction rolls back")
	}
}
