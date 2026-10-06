// Package httpapi is the REST delivery adapter: routing, middleware, DTOs and handlers.
// Handlers translate HTTP to use case calls and back; they contain no business rules.
package httpapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/usecase"
)

// OrderService is the use case surface the handlers depend on (an interface so tests can fake it).
type OrderService interface {
	Create(ctx context.Context, in usecase.CreateOrderInput) (*domain.Order, error)
	Get(ctx context.Context, id uuid.UUID) (*domain.Order, error)
	List(ctx context.Context, customerID uuid.UUID, cursor *domain.Cursor, limit int) (*usecase.ListResult, error)
	Cancel(ctx context.Context, id uuid.UUID) (*domain.Order, error)
}

type handler struct {
	svc  OrderService
	sub  OrderSubscriber
	log  *slog.Logger
	opts Options
}

func (h *handler) create(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := decodeJSON(r, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	items := make([]domain.Item, len(req.Items))
	for i, it := range req.Items {
		items[i] = domain.Item{SKU: it.SKU, Quantity: it.Quantity, UnitPrice: it.UnitPrice}
	}
	o, err := h.svc.Create(r.Context(), usecase.CreateOrderInput{
		CustomerID: req.CustomerID, Currency: req.Currency, Items: items,
	})
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	w.Header().Set("Location", "/v1/orders/"+o.ID.String())
	writeJSON(w, http.StatusCreated, toOrderResponse(o))
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	o, err := h.svc.Get(r.Context(), id)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	// Weak ETag from the optimistic-lock version: clients revalidate with If-None-Match and get
	// an empty 304 instead of the full body.
	etag := fmt.Sprintf(`W/"%d"`, o.Version)
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "private, no-cache")
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, toOrderResponse(o))
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	customerID, err := uuid.Parse(q.Get("customer_id"))
	if err != nil {
		writeProblem(w, r, http.StatusUnprocessableEntity, "Validation failed", "customer_id query parameter must be a UUID")
		return
	}
	cursor, err := decodeCursor(q.Get("cursor"))
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	limit := 0
	if s := q.Get("limit"); s != "" {
		if limit, err = strconv.Atoi(s); err != nil {
			writeProblem(w, r, http.StatusUnprocessableEntity, "Validation failed", "limit must be an integer")
			return
		}
	}

	res, err := h.svc.List(r.Context(), customerID, cursor, limit)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	out := listResponse{Data: make([]orderResponse, len(res.Orders)), NextCursor: encodeCursor(res.Next)}
	for i, o := range res.Orders {
		out.Data[i] = toOrderResponse(o)
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	o, err := h.svc.Cancel(r.Context(), id)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, toOrderResponse(o))
}

func parseID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "Invalid id", "id must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}
