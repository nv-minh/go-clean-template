package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/yourorg/go-clean-template/internal/platform/metrics"
)

// Options are the HTTP behaviours tuned per environment.
type Options struct {
	RequestTimeout    time.Duration
	MaxBodyBytes      int64
	CORSOrigins       []string
	TrustProxyHeaders bool
	SSEHeartbeat      time.Duration
	SSEMaxConnection  time.Duration
}

// Deps are everything the router needs. Optional collaborators may be nil (rate limiting and
// idempotency are then disabled), which keeps handler tests simple.
type Deps struct {
	Orders      OrderService
	Subscriber  OrderSubscriber
	Limiter     RateLimiter
	Idempotency IdempotencyStore
	Log         *slog.Logger
	Metrics     *metrics.Metrics
	Options     Options
}

// NewRouter assembles the public API.
//
// Middleware order matters (outermost first):
// tracing -> request id -> observe (metrics+logs) -> recover -> security headers -> CORS -> body limit.
// observe sits outside recover so that a recovered panic is still counted and logged as a 500.
func NewRouter(d Deps) http.Handler {
	h := &handler{svc: d.Orders, sub: d.Subscriber, log: d.Log, opts: d.Options}

	r := chi.NewRouter()
	r.Use(
		requestID,
		observe(d.Log, d.Metrics),
		recoverer(d.Log),
		secureHeaders,
		cors(d.Options.CORSOrigins),
		bodyLimit(d.Options.MaxBodyBytes),
	)

	limit := rateLimit(d.Limiter, d.Log, d.Options.TrustProxyHeaders)

	r.Route("/v1", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			r.Use(chimw.Timeout(d.Options.RequestTimeout), limit)
			r.With(idempotency(d.Idempotency, d.Log)).Post("/orders", h.create)
			r.Get("/orders", h.list)
			r.Get("/orders/{id}", h.get)
			r.Post("/orders/{id}/cancel", h.cancel)
		})
		// Long lived stream: no request timeout, but still rate limited on connect.
		if d.Subscriber != nil {
			r.With(limit).Get("/orders/{id}/stream", h.stream)
		}
	})

	return otelhttp.NewHandler(r, "http.server",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method }))
}
