## Dead Letter Events Table

The `dead_letter_events` table stores events that failed processing after:

- **MAX_RETRIES_EXCEEDED**: Event failed 3+ times with retryable errors
- **NON_RETRYABLE_ERROR**: Event failed with 4xx HTTP status or non-retryable error

### Reprocessing DLQ Events

To reprocess a DLQ event, you can manually insert it back into the outbox:

```sql
INSERT INTO outbox_events (aggregate_id, event_type, payload, created_at, retry_count)
SELECT aggregate_id, event_type, payload, NOW(), 0
FROM dead_letter_events
WHERE id = <dlq_event_id>;
```