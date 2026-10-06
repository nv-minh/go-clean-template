-- name: CreateOrder :exec
INSERT INTO orders (id, customer_id, status, currency, total_amount, items, version, created_at, updated_at)
VALUES (@id, @customer_id, @status, @currency, @total_amount, @items, @version, @created_at, @updated_at);

-- name: GetOrder :one
SELECT id, customer_id, status, currency, total_amount, items, version, created_at, updated_at
FROM orders
WHERE id = @id;

-- name: ListOrdersFirstPage :many
SELECT id, customer_id, status, currency, total_amount, items, version, created_at, updated_at
FROM orders
WHERE customer_id = @customer_id
ORDER BY created_at DESC, id DESC
LIMIT @page_size;

-- name: ListOrdersAfter :many
SELECT id, customer_id, status, currency, total_amount, items, version, created_at, updated_at
FROM orders
WHERE customer_id = @customer_id
  AND (created_at, id) < (@cursor_created_at::timestamptz, @cursor_id::uuid)
ORDER BY created_at DESC, id DESC
LIMIT @page_size;

-- Optimistic concurrency: affects 0 rows when another writer bumped the version first.
-- name: UpdateOrderStatus :execrows
UPDATE orders
SET status = @status, version = version + 1, updated_at = @updated_at
WHERE id = @id AND version = @version;
