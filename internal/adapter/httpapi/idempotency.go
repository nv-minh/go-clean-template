package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const (
	headerIdempotencyKey = "Idempotency-Key"
	maxIdempotencyKeyLen = 128
)

type IdemState int

const (
	IdemStarted  IdemState = iota // first time we see this key: run the handler
	IdemInFlight                  // another request with this key is still running
	IdemReplay                    // finished before: replay the saved response
	IdemMismatch                  // same key, different request payload
)

type SavedResponse struct {
	Status      int
	ContentType string
	Body        []byte
}

type IdemResult struct {
	State IdemState
	Saved *SavedResponse
}

// IdempotencyStore persists the outcome of requests keyed by the client supplied Idempotency-Key.
type IdempotencyStore interface {
	Begin(ctx context.Context, key, fingerprint string) (IdemResult, error)
	Complete(ctx context.Context, key, fingerprint string, resp SavedResponse) error
	Abort(ctx context.Context, key string) error
}

// idempotency implements the "Idempotency-Key" header contract used by payment APIs:
//   - same key + same payload  -> the original response is replayed (never executed twice);
//   - same key while running   -> 409 Conflict, client retries shortly;
//   - same key, other payload  -> 422, it is a client bug;
//   - 5xx outcomes are not stored, so the client can retry and really re-execute.
//
// Requests without the header pass through. If the store is down, keyed requests are refused
// with 503 rather than silently executed without the protection the client asked for.
func idempotency(store IdempotencyStore, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if store == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientKey := r.Header.Get(headerIdempotencyKey)
			if clientKey == "" {
				next.ServeHTTP(w, r)
				return
			}
			if len(clientKey) > maxIdempotencyKeyLen {
				writeProblem(w, r, http.StatusBadRequest, "Invalid Idempotency-Key", "key is too long")
				return
			}

			body, err := io.ReadAll(r.Body) // already bounded by bodyLimit
			if err != nil {
				writeDecodeError(w, r, err)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))

			sum := sha256.Sum256(append([]byte(r.Method+" "+r.URL.Path+"\n"), body...))
			fingerprint := hex.EncodeToString(sum[:])
			// TODO(auth): also scope the key by authenticated principal to prevent cross-tenant replay.
			key := r.Method + ":" + r.URL.Path + ":" + clientKey

			res, err := store.Begin(r.Context(), key, fingerprint)
			if err != nil {
				log.ErrorContext(r.Context(), "idempotency store unavailable", slog.Any("error", err))
				writeProblem(w, r, http.StatusServiceUnavailable, "Idempotency store unavailable", "retry later")
				return
			}
			switch res.State {
			case IdemInFlight:
				w.Header().Set("Retry-After", "1")
				writeProblem(w, r, http.StatusConflict, "Request in progress", "a request with this Idempotency-Key is still being processed")
				return
			case IdemMismatch:
				writeProblem(w, r, http.StatusUnprocessableEntity, "Idempotency-Key reused", "this key was used with a different request payload")
				return
			case IdemReplay:
				w.Header().Set("Content-Type", res.Saved.ContentType)
				w.Header().Set("Idempotent-Replayed", "true")
				w.WriteHeader(res.Saved.Status)
				_, _ = w.Write(res.Saved.Body)
				return
			case IdemStarted:
			}

			rec := &captureWriter{ResponseWriter: w, status: http.StatusOK}
			finished := false
			defer func() {
				// Detached: finish bookkeeping even if the client disconnected mid request.
				ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Second)
				defer cancel()
				if finished && rec.status < 500 {
					saved := SavedResponse{Status: rec.status, ContentType: w.Header().Get("Content-Type"), Body: rec.body.Bytes()}
					if err := store.Complete(ctx, key, fingerprint, saved); err != nil {
						log.WarnContext(ctx, "idempotency complete failed", slog.Any("error", err))
					}
					return
				}
				if err := store.Abort(ctx, key); err != nil { // also runs on panic
					log.WarnContext(ctx, "idempotency abort failed", slog.Any("error", err))
				}
			}()
			next.ServeHTTP(rec, r)
			finished = true
		})
	}
}

// captureWriter tees the response so it can be stored for replay.
type captureWriter struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (c *captureWriter) WriteHeader(code int) {
	c.status = code
	c.ResponseWriter.WriteHeader(code)
}

func (c *captureWriter) Write(b []byte) (int, error) {
	c.body.Write(b)
	return c.ResponseWriter.Write(b)
}

func (c *captureWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }
