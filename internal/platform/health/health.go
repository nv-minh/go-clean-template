// Package health implements Kubernetes style liveness and readiness probes.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// CheckFunc reports whether a dependency is usable.
type CheckFunc func(ctx context.Context) error

type check struct {
	name     string
	critical bool
	fn       CheckFunc
}

// Registry holds dependency checks.
type Registry struct {
	checks       []check
	shuttingDown atomic.Bool
}

// Add registers a check. A failing critical check makes /readyz return 503 (the pod leaves the
// load balancer). A failing non-critical check is only reported: use it for dependencies the
// service can degrade without (for example the cache), otherwise a Redis blip would eject every
// replica at once and turn a degradation into a full outage.
func (r *Registry) Add(name string, critical bool, fn CheckFunc) {
	r.checks = append(r.checks, check{name: name, critical: critical, fn: fn})
}

// MarkShuttingDown makes /readyz fail immediately so traffic drains before listeners close.
func (r *Registry) MarkShuttingDown() { r.shuttingDown.Store(true) }

// Live is the liveness probe: the process is up and the scheduler is running. It must not check
// dependencies, otherwise a database outage would restart healthy pods.
func (r *Registry) Live() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// Ready is the readiness probe.
func (r *Registry) Ready() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if r.shuttingDown.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "shutting_down"})
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()

		results := make(map[string]string, len(r.checks))
		var (
			mu      sync.Mutex
			wg      sync.WaitGroup
			healthy = true
		)
		for _, c := range r.checks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := c.fn(ctx)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					results[c.name] = "ok"
				case c.critical:
					results[c.name] = "fail: " + err.Error()
					healthy = false
				default:
					results[c.name] = "degraded: " + err.Error()
				}
			}()
		}
		wg.Wait()

		status, code := "ok", http.StatusOK
		if !healthy {
			status, code = "unavailable", http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]any{"status": status, "checks": results})
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
