-- PostgreSQL schema for the Play Outbox Pattern.

CREATE TABLE orders
(
    id           BIGSERIAL PRIMARY KEY,
    customer_id  VARCHAR(255)   NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    order_status VARCHAR(50)    NOT NULL DEFAULT 'PENDING',
    created_at   TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP      NOT NULL DEFAULT NOW(),
    deleted      BOOLEAN        NOT NULL DEFAULT FALSE -- Soft delete flag for UI filtering
);

CREATE TABLE outbox_events
(
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    VARCHAR(255) NOT NULL,
    event_type      VARCHAR(255) NOT NULL,
    payload         TEXT         NOT NULL,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING', -- PENDING, PROCESSING, PROCESSED, FAILED
    processed_at    TIMESTAMP,                          -- Set when event is successfully published or moved to DLQ
    retry_count     INT          NOT NULL DEFAULT 0,    -- Number of retry attempts
    last_error      TEXT,                               -- Last error encountered (preserved even after success for debugging)
    moved_to_dlq    BOOLEAN      NOT NULL DEFAULT FALSE, -- TRUE if event was moved to dead_letter_events table
    idempotency_key VARCHAR(512) NOT NULL              -- Unique key to prevent duplicate events: {aggregate_id}:{event_type}
);

-- Indexes for outbox_events
CREATE INDEX idx_outbox_status ON outbox_events (status);
CREATE INDEX idx_outbox_created ON outbox_events (created_at);
CREATE INDEX idx_outbox_aggregate ON outbox_events (aggregate_id);

-- Index for efficient polling of pending events (covers status + retry_count + created_at)
CREATE INDEX idx_outbox_pending ON outbox_events (status, retry_count, created_at)
    WHERE status IN ('PENDING', 'PROCESSING');

-- Unique constraint on idempotency key to prevent duplicate events
-- Only enforced for non-processed events to allow historical records
CREATE UNIQUE INDEX idx_outbox_idempotency ON outbox_events (idempotency_key)
    WHERE status != 'PROCESSED';

-- Dead letter queue table for failed events
CREATE TABLE dead_letter_events
(
    id                BIGSERIAL PRIMARY KEY,
    original_event_id BIGINT        NOT NULL,
    aggregate_id      VARCHAR(255)  NOT NULL,
    event_type        VARCHAR(255)  NOT NULL,
    payload           TEXT          NOT NULL,
    created_at        TIMESTAMP     NOT NULL,
    failed_at         TIMESTAMP     NOT NULL DEFAULT NOW(),
    retry_count       INT           NOT NULL,
    last_error        TEXT          NOT NULL,
    reason            VARCHAR(1024) NOT NULL
);

-- Index for querying DLQ by reason
CREATE INDEX idx_dlq_reason ON dead_letter_events (reason);
CREATE INDEX idx_dlq_failed_at ON dead_letter_events (failed_at DESC);
CREATE INDEX idx_dlq_original_event ON dead_letter_events (original_event_id);

-- PostgreSQL LISTEN/NOTIFY support for near-instant event processing
-- This trigger notifies the application when new events are inserted
CREATE OR REPLACE FUNCTION notify_new_outbox_event()
RETURNS trigger AS $$
BEGIN
  -- Send notification with event ID as payload
  PERFORM pg_notify('outbox_events_channel', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_event_inserted
AFTER INSERT ON outbox_events
FOR EACH ROW
EXECUTE FUNCTION notify_new_outbox_event();