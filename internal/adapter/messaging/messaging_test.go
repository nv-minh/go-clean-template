package messaging

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/platform/config"
	"github.com/yourorg/go-clean-template/internal/platform/metrics"
)

type fakePublisher struct {
	mu       sync.Mutex
	failures int // fail this many calls first
	topics   []string
	msgs     []Message
}

func (p *fakePublisher) Produce(_ context.Context, topic string, msgs []Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures > 0 {
		p.failures--
		return errors.New("broker unavailable")
	}
	for range msgs {
		p.topics = append(p.topics, topic)
	}
	p.msgs = append(p.msgs, msgs...)
	return nil
}

func newProc(h Handler, dlq Publisher, attempts int) *Processor {
	p := NewProcessor(h, dlq, "dlq", attempts, time.Millisecond, slog.New(slog.DiscardHandler), metrics.New("test"))
	p.dlqRetry = time.Millisecond
	return p
}

var rec = Record{Topic: "orders.events", Partition: 2, Offset: 41, Key: []byte("k"), Value: []byte("v"), Headers: map[string]string{}}

func TestProcessorRetriesTransientErrors(t *testing.T) {
	t.Parallel()
	calls := 0
	dlq := &fakePublisher{}
	p := newProc(func(context.Context, Record) error {
		calls++
		if calls < 3 {
			return errors.New("temporary")
		}
		return nil
	}, dlq, 5)
	if err := p.Process(t.Context(), rec); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || len(dlq.msgs) != 0 {
		t.Fatalf("calls=%d dlq=%d, want 3 calls and no DLQ", calls, len(dlq.msgs))
	}
}

func TestProcessorSendsPermanentErrorsStraightToDLQ(t *testing.T) {
	t.Parallel()
	for name, cause := range map[string]error{"ErrPermanent": ErrPermanent, "domain.ErrInvalid": domain.ErrInvalid} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			dlq := &fakePublisher{}
			p := newProc(func(context.Context, Record) error { calls++; return cause }, dlq, 5)
			if err := p.Process(t.Context(), rec); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || len(dlq.msgs) != 1 {
				t.Fatalf("calls=%d dlq=%d, want 1 and 1", calls, len(dlq.msgs))
			}
			h := dlq.msgs[0].Headers
			if h["x-original-topic"] != "orders.events" || h["x-original-partition"] != "2" || h["x-original-offset"] != "41" || h["x-error"] == "" {
				t.Fatalf("DLQ record lacks diagnostics: %v", h)
			}
		})
	}
}

func TestProcessorDeadLettersAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	calls := 0
	dlq := &fakePublisher{}
	p := newProc(func(context.Context, Record) error { calls++; return errors.New("always") }, dlq, 4)
	if err := p.Process(t.Context(), rec); err != nil {
		t.Fatal(err)
	}
	if calls != 4 || len(dlq.msgs) != 1 || dlq.topics[0] != "dlq" {
		t.Fatalf("calls=%d dlq=%d", calls, len(dlq.msgs))
	}
}

func TestProcessorNeverDropsWhenDLQIsDown(t *testing.T) {
	t.Parallel()
	dlq := &fakePublisher{failures: 3}
	p := newProc(func(context.Context, Record) error { return ErrPermanent }, dlq, 2)
	if err := p.Process(t.Context(), rec); err != nil {
		t.Fatal(err)
	}
	if len(dlq.msgs) != 1 {
		t.Fatal("record must reach the DLQ once it recovers")
	}
}

func TestProcessorStopsOnCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	p := newProc(func(context.Context, Record) error { cancel(); return errors.New("x") }, &fakePublisher{}, 5)
	if err := p.Process(ctx, rec); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestProcessorRecoversFromPanic(t *testing.T) {
	t.Parallel()
	dlq := &fakePublisher{}
	p := newProc(func(context.Context, Record) error { panic("bad message") }, dlq, 2)
	if err := p.Process(t.Context(), rec); err != nil || len(dlq.msgs) != 1 {
		t.Fatalf("panic must end in the DLQ, err=%v dlq=%d", err, len(dlq.msgs))
	}
}

type memClaims struct{ seen map[string]bool }

func (c *memClaims) Claim(_ context.Context, consumer, id string) (bool, error) {
	k := consumer + "/" + id
	if c.seen[k] {
		return false, nil
	}
	c.seen[k] = true
	return true, nil
}

type passTx struct{}

func (passTx) WithinTx(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

func TestIdempotentSkipsDuplicates(t *testing.T) {
	t.Parallel()
	calls := 0
	h := Idempotent("c", passTx{}, &memClaims{seen: map[string]bool{}}, func(context.Context, Record) error { calls++; return nil })
	r := Record{Headers: map[string]string{headerEventID: "7"}}
	for range 3 {
		if err := h(t.Context(), r); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times for the same event id, want 1", calls)
	}
	if err := h(t.Context(), Record{}); !errors.Is(err, ErrPermanent) {
		t.Fatalf("record without event id must be permanent, got %v", err)
	}
}

type fakeStore struct {
	events []domain.OutboxEvent
	marked bool
}

func (s *fakeStore) ProcessBatch(ctx context.Context, _ int, fn func(context.Context, []domain.OutboxEvent) error) (int, error) {
	if len(s.events) == 0 {
		return 0, nil
	}
	if err := fn(ctx, s.events); err != nil {
		return 0, err // like the real store: nothing is marked, the tx rolls back
	}
	s.marked = true
	return len(s.events), nil
}

func (s *fakeStore) DeletePublishedBefore(context.Context, time.Time) (int64, error) { return 0, nil }

func TestRelayPublishesWithKeyAndHeaders(t *testing.T) {
	t.Parallel()
	store := &fakeStore{events: []domain.OutboxEvent{{
		ID: 9, AggregateType: "order", AggregateID: "agg-1", EventType: "order.created", Payload: []byte(`{}`),
		Headers: map[string]string{"traceparent": "00-abc-def-01"},
	}}}
	pub := &fakePublisher{}
	r := NewRelay(store, pub, "orders.events", config.Outbox{BatchSize: 10}, slog.New(slog.DiscardHandler), metrics.New("test"))
	n, err := r.drainOnce(t.Context())
	if err != nil || n != 1 || !store.marked {
		t.Fatalf("n=%d err=%v marked=%v", n, err, store.marked)
	}
	m := pub.msgs[0]
	if m.Key != "agg-1" || m.Headers[headerEventID] != "9" || m.Headers[headerEventType] != "order.created" || m.Headers["traceparent"] == "" {
		t.Fatalf("unexpected message: %+v", m)
	}
}

func TestRelayDoesNotMarkWhenPublishFails(t *testing.T) {
	t.Parallel()
	store := &fakeStore{events: []domain.OutboxEvent{{ID: 1, AggregateID: "a"}}}
	r := NewRelay(store, &fakePublisher{failures: 1}, "t", config.Outbox{BatchSize: 10}, slog.New(slog.DiscardHandler), metrics.New("test"))
	if _, err := r.drainOnce(t.Context()); err == nil || store.marked {
		t.Fatalf("a failed publish must surface an error and leave events unpublished (err=%v marked=%v)", err, store.marked)
	}
}
