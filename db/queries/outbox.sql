-- name: InsertOutboxEvent :exec
INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, headers)
VALUES (@aggregate_type, @aggregate_id, @event_type, @payload, @headers);

-- Transaction scoped advisory lock: only one relay replica drains at a time, which keeps events
-- strictly ordered. The lock is released automatically on commit or rollback.
-- name: TryOutboxLock :one
SELECT pg_try_advisory_xact_lock(@key::bigint) AS locked;

-- name: ClaimOutboxBatch :many
SELECT id, aggregate_type, aggregate_id, event_type, payload, headers, created_at
FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT @batch_size;

-- name: MarkOutboxPublished :exec
UPDATE outbox SET published_at = now() WHERE id = ANY(@ids::bigint[]);

-- name: DeletePublishedOutbox :execrows
DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < @before::timestamptz;

-- name: ClaimProcessedEvent :execrows
INSERT INTO processed_events (consumer, event_id)
VALUES (@consumer, @event_id)
ON CONFLICT (consumer, event_id) DO NOTHING;
