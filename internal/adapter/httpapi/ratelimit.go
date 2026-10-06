package httpapi

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"
)

// RateLimiter decides whether the caller identified by key may proceed.
type RateLimiter interface {
	Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error)
}

// rateLimit rejects callers over their budget with 429 + Retry-After. It FAILS OPEN: if the
// limiter backend errors we serve the request. Rate limiting protects capacity; it must never
// become the reason the whole API is down.
func rateLimit(l RateLimiter, log *slog.Logger, trustProxy bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if l == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			allowed, retry, err := l.Allow(r.Context(), clientIP(r, trustProxy))
			if err != nil {
				log.WarnContext(r.Context(), "rate limiter unavailable, failing open", slog.Any("error", err))
				next.ServeHTTP(w, r)
				return
			}
			if !allowed {
				secs := max(1, int(math.Ceil(retry.Seconds())))
				w.Header().Set("Retry-After", strconv.Itoa(secs))
				writeProblem(w, r, http.StatusTooManyRequests, "Too many requests", "retry after "+strconv.Itoa(secs)+"s")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
