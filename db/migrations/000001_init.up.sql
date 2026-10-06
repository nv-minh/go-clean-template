CREATE TABLE orders (
    id           uuid        PRIMARY KEY,
    customer_id  uuid        NOT NULL,
    status       text        NOT NULL CHECK (status IN ('PENDING', 'PAID', 'CANCELLED')),
    currency     char(3)     NOT NULL,
    total_amount bigint      NOT NULL CHECK (total_amount >= 0),
    items        jsonb       NOT NULL,
    version      integer     NOT NULL DEFAULT 1,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL
);

-- Serves the keyset pagination query: WHERE customer_id = $1 AND (created_at, id) < ($2, $3)
-- ORDER BY created_at DESC, id DESC. The index order matches the sort, so no sort step is needed.
CREATE INDEX orders_customer_created_idx ON orders (customer_id, created_at DESC, id DESC);

-- Transactional outbox: written in the same transaction as the business change.
CREATE TABLE outbox (
    id             bigserial   PRIMARY KEY,
    aggregate_type text        NOT NULL,
    aggregate_id   text        NOT NULL,
    event_type     text        NOT NULL,
    payload        jsonb       NOT NULL,
    headers        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz
);

-- Partial index: stays tiny because it only contains rows still waiting to be published.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
-- Supports the retention cleanup of published rows.
CREATE INDEX outbox_published_at_idx ON outbox (published_at) WHERE published_at IS NOT NULL;

-- Consumer side deduplication: (consumer, event_id) is inserted in the same transaction as the
-- side effect, so a redelivered event is detected and skipped.
CREATE TABLE processed_events (
    consumer     text        NOT NULL,
    event_id     text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
