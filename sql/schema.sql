-- PostgreSQL schema for the Play Outbox Pattern.

CREATE TABLE orders
(
    id            BIGSERIAL PRIMARY KEY,
    customer_id   VARCHAR(255)   NOT NULL,
    total_amount  DECIMAL(10, 2) NOT NULL,
    shipping_type VARCHAR(20)    NOT NULL DEFAULT 'domestic', -- domestic or international
    order_status  VARCHAR(50)    NOT NULL DEFAULT 'PENDING',
    created_at    TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP      NOT NULL DEFAULT NOW(),
    deleted       BOOLEAN        NOT NULL DEFAULT FALSE       -- Soft delete flag for UI filtering
);

CREATE TABLE outbox_events
(
    id                BIGSERIAL PRIMARY KEY,
    aggregate_id      VARCHAR(255)             NOT NULL,
    event_type        VARCHAR(255)             NOT NULL,
    payloads          JSONB                    NOT NULL,                   -- Destination-specific payloads: {"destination-name": {...}}
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    status            VARCHAR(20)              NOT NULL DEFAULT 'PENDING', -- PENDING, PROCESSING, PROCESSED, FAILED
    status_changed_at TIMESTAMP WITH TIME ZONE,                            -- Timestamp when status last changed (for stale detection and completion tracking)
    retry_count       INT                      NOT NULL DEFAULT 0,         -- Number of retry attempts
    last_error        TEXT,                                                -- Last error encountered (preserved even after success for debugging)
    moved_to_dlq      BOOLEAN                  NOT NULL DEFAULT FALSE,     -- TRUE if event was moved to dead_letter_events table
    idempotency_key   VARCHAR(512)             NOT NULL,                   -- Unique key to prevent duplicate events: {aggregate_id}:{event_type}
    next_retry_at     TIMESTAMP WITH TIME ZONE                             -- When this event should be retried (NULL = ready now, stored in UTC)
);

-- Indexes for outbox_events
CREATE INDEX idx_outbox_status ON outbox_events (status);
CREATE INDEX idx_outbox_created ON outbox_events (created_at);
CREATE INDEX idx_outbox_aggregate ON outbox_events (aggregate_id);

-- Index for efficient polling of pending events (covers status + retry_count + next_retry_at + created_at)
CREATE INDEX idx_outbox_pending ON outbox_events (status, retry_count, next_retry_at, created_at)
    WHERE status IN ('PENDING', 'PROCESSING');

-- Unique constraint on idempotency key to prevent duplicate events
-- Only enforced for non-processed events to allow historical records
CREATE UNIQUE INDEX idx_outbox_idempotency ON outbox_events (idempotency_key)
    WHERE status != 'PROCESSED';

-- GIN index for efficient JSONB queries on payloads
CREATE INDEX idx_outbox_payloads_gin ON outbox_events USING GIN (payloads);

-- Dead letter queue table for failed events
-- Now includes status tracking for revert processing
CREATE TABLE dead_letter_events
(
    id                 BIGSERIAL PRIMARY KEY,
    original_event_id  BIGINT                   NOT NULL,
    aggregate_id       VARCHAR(255)             NOT NULL,
    event_type         VARCHAR(255)             NOT NULL,
    payloads           JSONB                    NOT NULL,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL,
    failed_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    retry_count        INT                      NOT NULL,
    last_error         TEXT                     NOT NULL,
    reason             VARCHAR(1024)            NOT NULL,
    -- Revert processing status
    status             VARCHAR(20)              NOT NULL DEFAULT 'PENDING', -- PENDING, PROCESSING, PROCESSED, FAILED
    status_changed_at  TIMESTAMP WITH TIME ZONE,                            -- When status last changed
    revert_retry_count INT                      NOT NULL DEFAULT 0,         -- Number of revert retry attempts
    revert_error       TEXT,                                                -- Last revert error
    next_retry_at      TIMESTAMP WITH TIME ZONE                             -- When to retry reverts (NULL = ready now)
);

-- Index for querying DLQ by reason
CREATE INDEX idx_dlq_reason ON dead_letter_events (reason);
CREATE INDEX idx_dlq_failed_at ON dead_letter_events (failed_at DESC);
CREATE INDEX idx_dlq_original_event ON dead_letter_events (original_event_id);

-- Index for pending DLQ events (for revert processing)
CREATE INDEX idx_dlq_pending ON dead_letter_events (status, revert_retry_count, next_retry_at, failed_at)
    WHERE status IN ('PENDING', 'PROCESSING');

-- GIN index for efficient JSONB queries on DLQ payloads
CREATE INDEX idx_dlq_payloads_gin ON dead_letter_events USING GIN (payloads);

-- Aggregate results table for tracking final publish status to each destination per aggregate
-- Stores the final result of publishing to each destination per aggregate (1:N relationship)
-- Uses UPSERT to keep only the final result per aggregate+destination, not all retry attempts or events
CREATE TABLE aggregate_results
(
    id               BIGSERIAL PRIMARY KEY,
    aggregate_id     VARCHAR(255)             NOT NULL, -- Business entity ID (e.g., order ID, customer ID)
    event_type       VARCHAR(255)             NOT NULL, -- Last event type sent (e.g., "OrderCreated", "OrderCancelled")
    destination      VARCHAR(255)             NOT NULL, -- Destination name (e.g., "inventory-webhook", "email-webhook")
    endpoint_url     VARCHAR(1024)            NOT NULL, -- Actual URL called
    http_method      VARCHAR(10)              NOT NULL, -- HTTP method (GET, POST, etc.)
    request_payload  JSONB,                             -- Request body sent
    response_status  INT,                               -- HTTP status code (200, 404, 500, etc.)
    response_payload JSONB,                             -- Response body received
    published_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    duration_ms      INT,                               -- Time taken for the request in milliseconds
    success          BOOLEAN                  NOT NULL, -- Whether the request was successful
    error_message    TEXT                               -- Error message if failed
);

-- Unique constraint to ensure only one result per aggregate+destination+event_type (enables UPSERT)
-- This allows both forward flow (e.g., "OrderCreated") and revert flow (e.g., "OrderCreated:REVERT")
-- results to coexist without overwriting each other
CREATE UNIQUE INDEX idx_aggregate_results_unique ON aggregate_results (aggregate_id, destination, event_type);

-- Index for querying by aggregate
CREATE INDEX idx_aggregate_results_aggregate_id ON aggregate_results (aggregate_id);

-- Index for querying by destination
CREATE INDEX idx_aggregate_results_destination ON aggregate_results (destination);

-- Index for querying by timestamp
CREATE INDEX idx_aggregate_results_published_at ON aggregate_results (published_at DESC);

-- Index for querying failures
CREATE INDEX idx_aggregate_results_failures ON aggregate_results (success, published_at DESC) WHERE success = false;

-- GIN index for efficient JSONB queries
CREATE INDEX idx_aggregate_results_request_gin ON aggregate_results USING GIN (request_payload);
CREATE INDEX idx_aggregate_results_response_gin ON aggregate_results USING GIN (response_payload);

-- PostgreSQL LISTEN/NOTIFY support for near-instant event processing
-- This trigger notifies the application when:
-- 1. New events are inserted (INSERT)
-- 2. Events are reset to PENDING and are ready now (next_retry_at is NULL or in the past)
-- It does NOT notify when status changes to PENDING with next_retry_at in the future (normal retry flow),
-- because the actor that processed the event will schedule the retry via getNextDueTime
CREATE OR REPLACE FUNCTION notify_new_outbox_event()
    RETURNS trigger AS
$$
BEGIN
    -- Notify when status is PENDING and either:
    -- 1. This is an INSERT, or
    -- 2. Status changed TO PENDING AND event is ready now (not scheduled for future)
    IF NEW.status = 'PENDING' AND (
        TG_OP = 'INSERT' OR
        (OLD.status IS DISTINCT FROM 'PENDING' AND (NEW.next_retry_at IS NULL OR NEW.next_retry_at <= NOW()))
        ) THEN
        PERFORM pg_notify('outbox_events_channel', NEW.id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_event_inserted
    AFTER INSERT OR UPDATE OF status
    ON outbox_events
    FOR EACH ROW
EXECUTE FUNCTION notify_new_outbox_event();

-- PostgreSQL LISTEN/NOTIFY support for DLQ events
-- This trigger notifies the application when:
-- 1. New DLQ events are inserted (events moved to DLQ)
-- 2. DLQ events are reset to PENDING and are ready now (next_retry_at is NULL or in the past)
-- It does NOT notify when status changes to PENDING with next_retry_at in the future (scheduled retry),
-- because the smart scheduling will wake up exactly when the event is due
CREATE OR REPLACE FUNCTION notify_dlq_event_ready()
    RETURNS trigger AS
$$
BEGIN
    -- Notify when status is PENDING and either:
    -- 1. New DLQ event inserted (ready immediately)
    -- 2. Status changed TO PENDING AND event is ready now (not scheduled for future)
    IF NEW.status = 'PENDING' AND (
        TG_OP = 'INSERT' OR
        (OLD.status IS DISTINCT FROM 'PENDING' AND (NEW.next_retry_at IS NULL OR NEW.next_retry_at <= NOW()))
        ) THEN
        PERFORM pg_notify('dlq_events_channel', NEW.id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dlq_event_ready
    AFTER INSERT OR UPDATE OF status
    ON dead_letter_events
    FOR EACH ROW
EXECUTE FUNCTION notify_dlq_event_ready();
