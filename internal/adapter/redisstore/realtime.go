package redisstore

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/yourorg/go-clean-template/internal/domain"
)

const (
	channelPrefix = "orders:"
	subBuffer     = 16
)

// Realtime is the Redis pub/sub hub.
//
// Publishing is fire and forget (no persistence, no replay): perfect for "push the latest state
// to whoever is watching", wrong for anything that must not be lost (use Kafka for that).
//
// To scale to many watchers each process opens ONE Redis subscription (pattern orders:*) and
// fans messages out to local subscribers in memory, instead of one Redis connection per client.
type Realtime struct {
	rdb *redis.Client
	log *slog.Logger

	mu   sync.RWMutex
	subs map[uuid.UUID]map[chan []byte]struct{}
}

var _ domain.Realtime = (*Realtime)(nil)

func NewRealtime(rdb *redis.Client, log *slog.Logger) *Realtime {
	return &Realtime{rdb: rdb, log: log, subs: make(map[uuid.UUID]map[chan []byte]struct{})}
}

func (r *Realtime) Publish(ctx context.Context, orderID uuid.UUID, payload []byte) error {
	if err := r.rdb.Publish(ctx, channelPrefix+orderID.String(), payload).Err(); err != nil {
		return fmt.Errorf("redis publish: %w", err)
	}
	return nil
}

// Run keeps the single process wide subscription alive until ctx is cancelled.
// go-redis reconnects and resubscribes automatically after a network failure.
func (r *Realtime) Run(ctx context.Context) error {
	ps := r.rdb.PSubscribe(ctx, channelPrefix+"*")
	defer func() { _ = ps.Close() }()
	if _, err := ps.Receive(ctx); err != nil {
		return fmt.Errorf("redis psubscribe: %w", err)
	}
	ch := ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			id, err := uuid.Parse(strings.TrimPrefix(msg.Channel, channelPrefix))
			if err != nil {
				continue
			}
			r.dispatch(id, []byte(msg.Payload))
		}
	}
}

func (r *Realtime) dispatch(id uuid.UUID, payload []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for ch := range r.subs[id] {
		select {
		case ch <- payload:
		default: // slow client: drop instead of blocking everyone else (it gets the next update)
			r.log.Debug("dropping realtime message for slow subscriber", slog.String("order_id", id.String()))
		}
	}
}

// Subscribe registers a local subscriber for one order. Call the returned func to unsubscribe.
func (r *Realtime) Subscribe(id uuid.UUID) (<-chan []byte, func()) {
	ch := make(chan []byte, subBuffer)
	r.mu.Lock()
	if r.subs[id] == nil {
		r.subs[id] = make(map[chan []byte]struct{})
	}
	r.subs[id][ch] = struct{}{}
	r.mu.Unlock()

	return ch, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.subs[id], ch)
		if len(r.subs[id]) == 0 {
			delete(r.subs, id)
		}
	}
}
