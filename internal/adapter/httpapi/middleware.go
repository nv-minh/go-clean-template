package httpapi

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/yourorg/go-clean-template/internal/platform/logger"
	"github.com/yourorg/go-clean-template/internal/platform/metrics"
)

const headerRequestID = "X-Request-ID"

// requestID reuses a sane inbound id (so ids flow across services) or generates one, echoes it
// in the response and stores it in the context for log correlation.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(headerRequestID)
		if id == "" || len(id) > 64 || strings.ContainsAny(id, " \t\r\n") {
			id = uuid.NewString()
		}
		w.Header().Set(headerRequestID, id)
		next.ServeHTTP(w, r.WithContext(logger.WithRequestID(r.Context(), id)))
	})
}

func routePattern(r *http.Request) string {
	if rc := chi.RouteContext(r.Context()); rc != nil {
		if p := rc.RoutePattern(); p != "" {
			return p
		}
	}
	return "unmatched"
}

// observe records RED metrics, names the trace span after the route pattern (low cardinality)
// and writes one structured access log line per request.
func observe(log *slog.Logger, m *metrics.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
			m.HTTPInFlight.Inc()
			defer m.HTTPInFlight.Dec()

			next.ServeHTTP(ww, r)

			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			route := routePattern(r)
			elapsed := time.Since(start)
			m.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
			m.HTTPDuration.WithLabelValues(r.Method, route).Observe(elapsed.Seconds())

			span := trace.SpanFromContext(r.Context())
			span.SetName(r.Method + " " + route)
			span.SetAttributes(attribute.String("http.route", route))

			level := slog.LevelInfo
			if status >= 500 {
				level = slog.LevelError
			}
			log.LogAttrs(r.Context(), level, "http request",
				slog.String("method", r.Method),
				slog.String("route", route),
				slog.String("path", r.URL.Path),
				slog.Int("status", status),
				slog.Int("bytes", ww.BytesWritten()),
				slog.Duration("duration", elapsed),
				slog.String("remote_ip", clientIP(r, false)),
				slog.String("user_agent", r.UserAgent()),
			)
		})
	}
}

// recoverer turns a handler panic into a 500 instead of a dropped connection, and logs the stack.
func recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				if rec == http.ErrAbortHandler { //nolint:errorlint,err113 // sentinel panic value by net/http contract
					panic(rec)
				}
				log.ErrorContext(r.Context(), "panic recovered",
					slog.String("panic", fmt.Sprint(rec)), slog.String("stack", string(debug.Stack())))
				writeProblem(w, r, http.StatusInternalServerError, "Internal server error", "")
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// secureHeaders sets conservative defaults for a JSON API.
func secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		if h.Get("Cache-Control") == "" {
			h.Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

// bodyLimit caps request bodies; handlers then see *http.MaxBytesError and answer 413.
func bodyLimit(n int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler { return http.MaxBytesHandler(next, n) }
}

// cors allows an explicit origin allow-list. An empty list disables CORS headers entirely
// (same-origin / server to server use), which is the safe default.
func cors(origins []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if len(origins) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" && (slices.Contains(origins, "*") || slices.Contains(origins, origin)) {
				h := w.Header()
				h.Set("Access-Control-Allow-Origin", origin)
				h.Add("Vary", "Origin")
				if r.Method == http.MethodOptions {
					h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
					h.Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key, X-Request-ID, If-None-Match")
					h.Set("Access-Control-Max-Age", "600")
					w.WriteHeader(http.StatusNoContent)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// clientIP returns the caller address. Forwarding headers are spoofable by clients, so they are
// only honoured when the service runs behind a trusted proxy (HTTP_TRUST_PROXY_HEADERS=true).
func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if v := r.Header.Get("X-Real-IP"); v != "" {
			return v
		}
		if v := r.Header.Get("X-Forwarded-For"); v != "" {
			first, _, _ := strings.Cut(v, ",")
			return strings.TrimSpace(first)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
