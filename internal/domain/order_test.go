package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/yourorg/go-clean-template/internal/domain"
)

var (
	now      = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	customer = uuid.MustParse("0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
)

func TestNewOrder(t *testing.T) {
	t.Parallel()
	valid := []domain.Item{{SKU: "A", Quantity: 2, UnitPrice: 150}, {SKU: "B", Quantity: 1, UnitPrice: 99}}

	tests := []struct {
		name     string
		customer uuid.UUID
		currency string
		items    []domain.Item
		wantErr  bool
		wantSum  int64
	}{
		{name: "valid", customer: customer, currency: "usd", items: valid, wantSum: 399},
		{name: "missing customer", customer: uuid.Nil, currency: "USD", items: valid, wantErr: true},
		{name: "bad currency", customer: customer, currency: "US", items: valid, wantErr: true},
		{name: "no items", customer: customer, currency: "USD", wantErr: true},
		{name: "blank sku", customer: customer, currency: "USD", items: []domain.Item{{SKU: " ", Quantity: 1}}, wantErr: true},
		{name: "zero quantity", customer: customer, currency: "USD", items: []domain.Item{{SKU: "A", Quantity: 0}}, wantErr: true},
		{name: "negative price", customer: customer, currency: "USD", items: []domain.Item{{SKU: "A", Quantity: 1, UnitPrice: -1}}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o, err := domain.NewOrder(tc.customer, tc.currency, tc.items, now)
			if tc.wantErr {
				if !errors.Is(err, domain.ErrInvalid) {
					t.Fatalf("want ErrInvalid, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if o.TotalAmount != tc.wantSum || o.Status != domain.StatusPending || o.Version != 1 || o.Currency != "USD" {
				t.Fatalf("unexpected order: %+v", o)
			}
			if o.ID.Version() != 7 {
				t.Fatalf("want UUIDv7 id, got version %d", o.ID.Version())
			}
		})
	}
}

func TestOrderCancel(t *testing.T) {
	t.Parallel()
	o, err := domain.NewOrder(customer, "USD", []domain.Item{{SKU: "A", Quantity: 1}}, now)
	if err != nil {
		t.Fatal(err)
	}
	later := now.Add(time.Minute)
	if err := o.Cancel(later); err != nil {
		t.Fatalf("cancel pending: %v", err)
	}
	if o.Status != domain.StatusCancelled || !o.UpdatedAt.Equal(later) {
		t.Fatalf("unexpected state: %+v", o)
	}
	if err := o.Cancel(later); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("cancelling twice must fail with ErrInvalidState, got %v", err)
	}
}
