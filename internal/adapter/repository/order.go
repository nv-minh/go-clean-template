package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/yourorg/go-clean-template/internal/adapter/repository/sqlcdb"
	"github.com/yourorg/go-clean-template/internal/domain"
)

type OrderRepository struct{ store *Store }

var _ domain.OrderRepository = (*OrderRepository)(nil)

func NewOrderRepository(store *Store) *OrderRepository { return &OrderRepository{store: store} }

func (r *OrderRepository) Create(ctx context.Context, o *domain.Order) error {
	items, err := json.Marshal(o.Items)
	if err != nil {
		return fmt.Errorf("marshal items: %w", err)
	}
	err = r.store.queries(ctx).CreateOrder(ctx, sqlcdb.CreateOrderParams{
		ID:          o.ID,
		CustomerID:  o.CustomerID,
		Status:      string(o.Status),
		Currency:    o.Currency,
		TotalAmount: o.TotalAmount,
		Items:       items,
		Version:     o.Version,
		CreatedAt:   o.CreatedAt,
		UpdatedAt:   o.UpdatedAt,
	})
	return mapErr(err)
}

func (r *OrderRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	row, err := r.store.queries(ctx).GetOrder(ctx, id)
	if err != nil {
		return nil, mapErr(err)
	}
	return toDomain(row)
}

func (r *OrderRepository) List(ctx context.Context, p domain.ListParams) ([]*domain.Order, error) {
	q := r.store.queries(ctx)
	var (
		rows []sqlcdb.Order
		err  error
	)
	if p.Cursor == nil {
		rows, err = q.ListOrdersFirstPage(ctx, sqlcdb.ListOrdersFirstPageParams{
			CustomerID: p.CustomerID,
			PageSize:   int32(p.Limit), //nolint:gosec // limit is capped by the use case
		})
	} else {
		rows, err = q.ListOrdersAfter(ctx, sqlcdb.ListOrdersAfterParams{
			CustomerID:      p.CustomerID,
			CursorCreatedAt: p.Cursor.CreatedAt,
			CursorID:        p.Cursor.ID,
			PageSize:        int32(p.Limit), //nolint:gosec // limit is capped by the use case
		})
	}
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Order, 0, len(rows))
	for _, row := range rows {
		o, err := toDomain(row)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}

func (r *OrderRepository) UpdateStatus(ctx context.Context, o *domain.Order) error {
	n, err := r.store.queries(ctx).UpdateOrderStatus(ctx, sqlcdb.UpdateOrderStatusParams{
		Status:    string(o.Status),
		UpdatedAt: o.UpdatedAt,
		ID:        o.ID,
		Version:   o.Version,
	})
	if err != nil {
		return mapErr(err)
	}
	if n == 0 {
		return fmt.Errorf("%w: order %s was modified concurrently", domain.ErrConflict, o.ID)
	}
	o.Version++
	return nil
}

func toDomain(row sqlcdb.Order) (*domain.Order, error) {
	var items []domain.Item
	if err := json.Unmarshal(row.Items, &items); err != nil {
		return nil, fmt.Errorf("unmarshal items of order %s: %w", row.ID, err)
	}
	return &domain.Order{
		ID:          row.ID,
		CustomerID:  row.CustomerID,
		Status:      domain.Status(row.Status),
		Currency:    row.Currency,
		TotalAmount: row.TotalAmount,
		Items:       items,
		Version:     row.Version,
		CreatedAt:   row.CreatedAt.UTC(),
		UpdatedAt:   row.UpdatedAt.UTC(),
	}, nil
}
