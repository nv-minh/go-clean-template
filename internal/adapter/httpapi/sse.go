package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// OrderSubscriber delivers realtime updates of one order to this process.
type OrderSubscriber interface {
	Subscribe(id uuid.UUID) (<-chan []byte, func())
}

// stream is a Server-Sent Events endpoint: GET /v1/orders/{id}/stream.
// Updates are produced by the worker (Kafka -> Redis pub/sub) and reach clients connected to any API replica.
func (h *handler) stream(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	// Check existence (and send the current state) before holding a long lived connection.
	o, err := h.svc.Get(r.Context(), id)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}

	rc := http.NewResponseController(w)
	// The server wide WriteTimeout would kill a long lived stream; lift it for this response only.
	if err := rc.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		writeError(w, r, h.log, err)
		return
	}

	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)
	w.WriteHeader(http.StatusOK)

	// Subscribe BEFORE sending the snapshot so no update falls in the gap between the two.
	updates, unsubscribe := h.sub.Subscribe(id)
	defer unsubscribe()

	if !writeEvent(w, rc, "snapshot", mustJSON(toOrderResponse(o))) {
		return
	}

	heartbeat := time.NewTicker(h.opts.SSEHeartbeat)
	defer heartbeat.Stop()
	// Bounded lifetime: forces clients to reconnect periodically, which rebalances streams across
	// replicas after scale-out and prevents leaked connections from living forever.
	ctx, cancel := context.WithTimeout(r.Context(), h.opts.SSEMaxConnection)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-updates:
			if !writeEvent(w, rc, "update", msg) {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil || rc.Flush() != nil {
				return
			}
		}
	}
}

func writeEvent(w http.ResponseWriter, rc *http.ResponseController, event string, data []byte) bool {
	// text/event-stream, not HTML; data is JSON produced by this service.
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data); err != nil { //nolint:gosec // G705 false positive, see comment above
		return false
	}
	return rc.Flush() == nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}
