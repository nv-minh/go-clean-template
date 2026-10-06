package httpapi

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/yourorg/go-clean-template/internal/domain"
)

type itemDTO struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	UnitPrice int64  `json:"unit_price"`
}

type createOrderRequest struct {
	CustomerID uuid.UUID `json:"customer_id"`
	Currency   string    `json:"currency"`
	Items      []itemDTO `json:"items"`
}

type orderResponse struct {
	ID          uuid.UUID     `json:"id"`
	CustomerID  uuid.UUID     `json:"customer_id"`
	Status      domain.Status `json:"status"`
	Currency    string        `json:"currency"`
	TotalAmount int64         `json:"total_amount"`
	Items       []itemDTO     `json:"items"`
	Version     int32         `json:"version"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

type listResponse struct {
	Data       []orderResponse `json:"data"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

func toOrderResponse(o *domain.Order) orderResponse {
	items := make([]itemDTO, len(o.Items))
	for i, it := range o.Items {
		items[i] = itemDTO{SKU: it.SKU, Quantity: it.Quantity, UnitPrice: it.UnitPrice}
	}
	return orderResponse{
		ID: o.ID, CustomerID: o.CustomerID, Status: o.Status, Currency: o.Currency,
		TotalAmount: o.TotalAmount, Items: items, Version: o.Version,
		CreatedAt: o.CreatedAt, UpdatedAt: o.UpdatedAt,
	}
}

// Cursors are opaque to clients: base64url("<unix nano>|<uuid>"). Clients must not parse them,
// which leaves us free to change the sort key later.
func encodeCursor(c *domain.Cursor) string {
	if c == nil {
		return ""
	}
	raw := strconv.FormatInt(c.CreatedAt.UnixNano(), 10) + "|" + c.ID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (*domain.Cursor, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed cursor", domain.ErrInvalid)
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return nil, fmt.Errorf("%w: malformed cursor", domain.ErrInvalid)
	}
	nanos, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed cursor", domain.ErrInvalid)
	}
	uid, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed cursor", domain.ErrInvalid)
	}
	return &domain.Cursor{CreatedAt: time.Unix(0, nanos).UTC(), ID: uid}, nil
}
