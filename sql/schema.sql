-- PostgreSQL schema for the Play Outbox Pattern.

CREATE TABLE orders
(
    id           BIGSERIAL PRIMARY KEY,
    customer_id  VARCHAR(255)   NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status       VARCHAR(50)    NOT NULL DEFAULT 'PENDING',
    created_at   TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP      NOT NULL DEFAULT NOW(),
    deleted      BOOLEAN        NOT NULL DEFAULT FALSE -- Soft delete flag for UI filtering
);

CREATE TABLE outbox_events
(
    id           BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type   VARCHAR(255) NOT NULL,
    payload      TEXT         NOT NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP,                          -- Set when event is successfully published or moved to DLQ
    retry_count  INT          NOT NULL DEFAULT 0,    -- Number of retry attempts
    last_error   TEXT,                               -- Last error encountered (preserved even after success for debugging)
    moved_to_dlq BOOLEAN      NOT NULL DEFAULT FALSE -- TRUE if event was moved to dead_letter_events table
);

-- Indexes for outbox_events
CREATE INDEX idx_outbox_processed ON outbox_events (processed_at);
CREATE INDEX idx_outbox_created ON outbox_events (created_at);
CREATE INDEX idx_outbox_aggregate ON outbox_events (aggregate_id);

-- Index for efficient polling of unprocessed events
CREATE INDEX idx_outbox_unprocessed ON outbox_events (created_at)
    WHERE processed_at IS NULL AND retry_count < 3;

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