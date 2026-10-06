package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yourorg/go-clean-template/internal/adapter/httpapi"
)

// processingTTL bounds how long a crashed request can block retries with the same key.
const processingTTL = 60 * time.Second

// Idempotency implements httpapi.IdempotencyStore. It lets clients safely retry POST requests
// (network timeouts, mobile reconnects) without creating duplicates.
type Idempotency struct {
	rdb *redis.Client
	ttl time.Duration
}

var _ httpapi.IdempotencyStore = (*Idempotency)(nil)

func NewIdempotency(rdb *redis.Client, ttl time.Duration) *Idempotency {
	return &Idempotency{rdb: rdb, ttl: ttl}
}

type record struct {
	Done        bool   `json:"done"`
	Fingerprint string `json:"fp"`
	Status      int    `json:"status,omitempty"`
	ContentType string `json:"ct,omitempty"`
	Body        []byte `json:"body,omitempty"`
}

func idemKey(k string) string { return "idem:" + k }

func (s *Idempotency) Begin(ctx context.Context, key, fingerprint string) (httpapi.IdemResult, error) {
	rk := idemKey(key)
	for range 2 { // second pass only if the record expired between SETNX and GET
		raw, _ := json.Marshal(record{Fingerprint: fingerprint})
		ok, err := s.rdb.SetNX(ctx, rk, raw, processingTTL).Result()
		if err != nil {
			return httpapi.IdemResult{}, fmt.Errorf("idempotency setnx: %w", err)
		}
		if ok {
			return httpapi.IdemResult{State: httpapi.IdemStarted}, nil
		}

		stored, err := s.rdb.Get(ctx, rk).Bytes()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return httpapi.IdemResult{}, fmt.Errorf("idempotency get: %w", err)
		}
		var rec record
		if err := json.Unmarshal(stored, &rec); err != nil {
			return httpapi.IdemResult{}, fmt.Errorf("idempotency decode: %w", err)
		}
		switch {
		case rec.Fingerprint != fingerprint:
			return httpapi.IdemResult{State: httpapi.IdemMismatch}, nil
		case !rec.Done:
			return httpapi.IdemResult{State: httpapi.IdemInFlight}, nil
		default:
			return httpapi.IdemResult{State: httpapi.IdemReplay, Saved: &httpapi.SavedResponse{
				Status: rec.Status, ContentType: rec.ContentType, Body: rec.Body,
			}}, nil
		}
	}
	return httpapi.IdemResult{State: httpapi.IdemInFlight}, nil
}

func (s *Idempotency) Complete(ctx context.Context, key, fingerprint string, resp httpapi.SavedResponse) error {
	raw, err := json.Marshal(record{Done: true, Fingerprint: fingerprint, Status: resp.Status, ContentType: resp.ContentType, Body: resp.Body})
	if err != nil {
		return fmt.Errorf("idempotency encode: %w", err)
	}
	return s.rdb.Set(ctx, idemKey(key), raw, s.ttl).Err()
}

func (s *Idempotency) Abort(ctx context.Context, key string) error {
	return s.rdb.Del(ctx, idemKey(key)).Err()
}
