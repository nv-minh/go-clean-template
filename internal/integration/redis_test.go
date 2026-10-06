//go:build integration

package integration

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/yourorg/go-clean-template/internal/adapter/httpapi"
	"github.com/yourorg/go-clean-template/internal/adapter/redisstore"
	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/platform/metrics"
)

func TestOrderCache(t *testing.T) {
	c := redisstore.NewOrderCache(rdb, time.Minute, metrics.New("it"))
	o := newOrder(t, uuid.New(), time.Now().UTC().Truncate(time.Microsecond))

	if _, err := c.Get(t.Context(), o.ID); !errors.Is(err, domain.ErrCacheMiss) {
		t.Fatalf("want miss, got %v", err)
	}
	if err := c.Set(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(t.Context(), o.ID)
	if err != nil || got.ID != o.ID || got.TotalAmount != o.TotalAmount || len(got.Items) != 1 || !got.CreatedAt.Equal(o.CreatedAt) {
		t.Fatalf("round trip mismatch: %+v err=%v", got, err)
	}
	if err := c.Delete(t.Context(), o.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(t.Context(), o.ID); !errors.Is(err, domain.ErrCacheMiss) {
		t.Fatal("deleted key must miss")
	}
}

func TestRateLimiterTokenBucket(t *testing.T) {
	l := redisstore.NewRateLimiter(rdb, 5, 3) // 5 tokens/s, burst 3
	key := uuid.NewString()

	for i := range 3 {
		if ok, _, err := l.Allow(t.Context(), key); err != nil || !ok {
			t.Fatalf("request %d within burst must pass (ok=%v err=%v)", i, ok, err)
		}
	}
	ok, retry, err := l.Allow(t.Context(), key)
	if err != nil || ok || retry <= 0 || retry > time.Second {
		t.Fatalf("4th request must be rejected with a sane retry-after (ok=%v retry=%v err=%v)", ok, retry, err)
	}
	if ok, _, _ := l.Allow(t.Context(), uuid.NewString()); !ok {
		t.Fatal("limits must be per key")
	}
	time.Sleep(300 * time.Millisecond) // refills ~1.5 tokens
	if ok, _, _ := l.Allow(t.Context(), key); !ok {
		t.Fatal("tokens must refill over time")
	}
}

func TestIdempotencyStore(t *testing.T) {
	s := redisstore.NewIdempotency(rdb, time.Minute)
	key := "POST:/v1/orders:" + uuid.NewString()

	if r, err := s.Begin(t.Context(), key, "fp1"); err != nil || r.State != httpapi.IdemStarted {
		t.Fatalf("first: %+v %v", r, err)
	}
	if r, _ := s.Begin(t.Context(), key, "fp1"); r.State != httpapi.IdemInFlight {
		t.Fatalf("while running want InFlight, got %v", r.State)
	}
	if r, _ := s.Begin(t.Context(), key, "other"); r.State != httpapi.IdemMismatch {
		t.Fatalf("different payload want Mismatch, got %v", r.State)
	}
	saved := httpapi.SavedResponse{Status: 201, ContentType: "application/json", Body: []byte(`{"id":1}`)}
	if err := s.Complete(t.Context(), key, "fp1", saved); err != nil {
		t.Fatal(err)
	}
	r, _ := s.Begin(t.Context(), key, "fp1")
	if r.State != httpapi.IdemReplay || r.Saved.Status != 201 || string(r.Saved.Body) != `{"id":1}` {
		t.Fatalf("want replay of saved response, got %+v", r)
	}

	key2 := key + "-abort"
	_, _ = s.Begin(t.Context(), key2, "fp")
	if err := s.Abort(t.Context(), key2); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Begin(t.Context(), key2, "fp"); r.State != httpapi.IdemStarted {
		t.Fatal("aborted key must be usable again")
	}
}

func TestRealtimeFanOut(t *testing.T) {
	rt := redisstore.NewRealtime(rdb, slog.New(slog.DiscardHandler))
	go func() { _ = rt.Run(t.Context()) }()
	time.Sleep(300 * time.Millisecond) // let PSUBSCRIBE settle

	id := uuid.New()
	a, unsubA := rt.Subscribe(id)
	b, unsubB := rt.Subscribe(id)
	defer unsubB()
	other, unsubOther := rt.Subscribe(uuid.New())
	defer unsubOther()

	if err := rt.Publish(t.Context(), id, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	for name, ch := range map[string]<-chan []byte{"a": a, "b": b} {
		select {
		case msg := <-ch:
			if string(msg) != "hello" {
				t.Fatalf("%s got %q", name, msg)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("subscriber %s did not receive the message", name)
		}
	}
	select {
	case <-other:
		t.Fatal("subscriber of another order must not receive it")
	case <-time.After(200 * time.Millisecond):
	}

	unsubA()
	_ = rt.Publish(t.Context(), id, []byte("again"))
	select {
	case <-b:
	case <-time.After(3 * time.Second):
		t.Fatal("remaining subscriber must still receive")
	}
}
