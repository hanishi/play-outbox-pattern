package repositories
import com.google.inject.{ Inject, Singleton }
import models.{ DestinationConfig, EventStatus, OutboxEvent }
import play.api.libs.json.*
import repositories.OutboxEventsPostgresProfile.api.*
import slick.jdbc.GetResult

import java.time.Instant
import scala.concurrent.ExecutionContext

class OutboxTable(tag: Tag) extends Table[OutboxEvent](tag, "outbox_events") {
  import CommonMappers.given

  def * = (
    id,
    aggregateId,
    eventType,
    payloads,
    createdAt,
    status,
    statusChangedAt,
    retryCount,
    lastError,
    movedToDlq,
    idempotencyKey,
    nextRetryAt
  ).mapTo[OutboxEvent]

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def aggregateId = column[String]("aggregate_id")

  def eventType = column[String]("event_type")

  def payloads = column[Map[String, DestinationConfig]]("payloads")

  def createdAt = column[Instant]("created_at")

  def status = column[EventStatus]("status")

  def statusChangedAt = column[Option[Instant]]("status_changed_at")

  def retryCount = column[Int]("retry_count")

  def lastError = column[Option[String]]("last_error")

  def movedToDlq = column[Boolean]("moved_to_dlq")

  def idempotencyKey = column[String]("idempotency_key")

  def nextRetryAt = column[Option[Instant]]("next_retry_at")
}

/** Repository for managing outbox events and their lifecycle.
  *
  * == Purpose ==
  * Central persistence layer for the Transactional Outbox Pattern. Manages event lifecycle from
  * creation (PENDING) through processing (PROCESSING) to completion (PROCESSED) or failure (DLQ).
  *
  * == Event Lifecycle ==
  * {{{
  * PENDING → PROCESSING → PROCESSED (success)
  *    ↓
  *    ↓ (after max retries)
  *    ↓
  *  DLQ → Automatic Compensation (LIFO revert)
  * }}}
  *
  * == Core Responsibilities ==
  * 1. '''Event Storage''': Persist events atomically with business operations via OutboxHelper
  *
  * 2. '''Event Claiming''': Atomically claim pending events for processing (FOR UPDATE SKIP LOCKED)
  *
  * 3. '''Retry Management''': Track retry counts and schedule retries with exponential backoff
  *
  * 4. '''DLQ Movement''': Move exhausted events to the dead letter queue for compensation
  *
  * 5. '''Stale Recovery''': Reset stuck PROCESSING events back to PENDING (crash recovery)
  *
  * == Concurrency Control ==
  * Uses PostgreSQL's FOR UPDATE SKIP LOCKED to prevent duplicate processing across multiple workers:
  * {{{
  * # Scenario: 3 OutboxProcessor instances running concurrently
  *
  * T1: Worker A calls findAndClaimUnprocessed(limit=100)
  *     → Locks events [1, 2, 3] with FOR UPDATE SKIP LOCKED
  *     → Updates status to PROCESSING
  *     → Returns [1, 2, 3]
  *
  * T2: Worker B calls findAndClaimUnprocessed(limit=100) (concurrent with T1)
  *     → FOR UPDATE SKIP LOCKED skips [1, 2, 3] (already locked by A)
  *     → Locks events [4, 5, 6]
  *     → Returns [4, 5, 6]
  *
  * T3: Worker C calls findAndClaimUnprocessed(limit=100) (concurrent with T1, T2)
  *     → Skips [1, 2, 3, 4, 5, 6]
  *     → Locks events [7, 8]
  *     → Returns [7, 8]
  *
  * # Result: No duplicate processing, all workers process different events
  * }}}
  *
  * == Retry Strategy ==
  * {{{
  * # Exponential backoff with configurable max retries (default: 3)
  *
  * # Attempt 1: Immediate
  * INSERT INTO outbox_events (status='PENDING', retry_count=0, next_retry_at=NULL)
  * # → OutboxProcessor picks up immediately
  *
  * # Attempt 2: After 2 seconds (2^1)
  * # API fails with 503
  * incrementRetryCount(id, currentRetryCount=0, error="HTTP 503", nextRetryAt=now+2s)
  * # → UPDATE SET retry_count=1, status='PENDING', next_retry_at=T+2s
  *
  * # Attempt 3: After 4 seconds (2^2)
  * # API fails with 503
  * incrementRetryCount(id, currentRetryCount=1, error="HTTP 503", nextRetryAt=now+4s)
  * # → UPDATE SET retry_count=2, status='PENDING', next_retry_at=T+4s
  *
  * # Attempt 4: After 8 seconds (2^3)
  * # API fails with 503, max retries exceeded
  * moveToDLQ(event, reason="MAX_RETRIES_EXCEEDED", error="HTTP 503")
  * # → INSERT INTO dead_letter_events (...)
  * # → UPDATE outbox_events SET status='PROCESSED', moved_to_dlq=true
  * # → Triggers automatic compensation
  * }}}
  *
  * == Stale Event Recovery ==
  * {{{
  * # Events can get stuck in PROCESSING if worker crashes
  *
  * T1: Event marked PROCESSING, worker starts processing
  * T2: Worker crashes (network partition, OOM, etc.)
  * T3: Event stuck in PROCESSING status forever
  *
  * # Solution: Periodic cleanup job
  * resetStaleProcessingEvents(timeoutMinutes=5)
  * # → SELECT * WHERE status='PROCESSING' AND status_changed_at < NOW() - 5 minutes
  * # → UPDATE SET status='PENDING' (back to queue for retry)
  * }}}
  *
  * == Idempotency ==
  * Events have unique idempotency keys (aggregateId:eventType) to prevent duplicates:
  * {{{
  * CREATE UNIQUE INDEX ON outbox_events (idempotency_key) WHERE status != 'PROCESSED'
  *
  * # Prevents duplicate events:
  * INSERT INTO outbox_events (idempotency_key='123:OrderCreated', ...)  # Succeeds
  * INSERT INTO outbox_events (idempotency_key='123:OrderCreated', ...)  # Fails (duplicate)
  * }}}
  */
@Singleton
class OutboxRepository @Inject() (
    dlqRepo: DeadLetterRepository,
    config: play.api.Configuration
)(using ec: ExecutionContext) {
  import CommonMappers.given

  val outbox = TableQuery[OutboxTable]

  private val maxRetries = config.getOptional[Int]("outbox.maxRetries").getOrElse(2)

  /** Inserts a new outbox event.
    *
    * Called by OutboxHelper when business operations create events.
    * Event starts with status=PENDING and will be picked up by OutboxProcessor.
    *
    * @param event The outbox event to insert
    * @return DBIO action that returns the generated event ID
    */
  def insert(event: OutboxEvent): DBIO[Long] = outbox returning outbox.map(_.id) += event

  /** Finds a single outbox event by ID.
    *
    * Used for debugging and diagnostics.
    *
    * @param id The event ID
    * @return DBIO action that returns the event
    * @throws NoSuchElementException if event not found
    */
  def find(id: Long): DBIO[OutboxEvent] =
    outbox.filter(_.id === id).result.head

  /** Atomically claims and returns pending events for processing.
    *
    * Core method used by OutboxProcessor to claim events for processing. Uses PostgreSQL's
    * FOR UPDATE SKIP LOCKED to prevent duplicate processing across multiple workers.
    *
    * == Query Breakdown ==
    * {{{
    * WITH claimed AS (
    *   SELECT id
    *   FROM outbox_events
    *   WHERE status = 'PENDING'                          -- Ready to process
    *     AND (next_retry_at IS NULL OR next_retry_at <= NOW())  -- Not waiting for retry delay
    *   ORDER BY created_at                               -- FIFO: oldest first
    *   LIMIT 100                                         -- Batch size
    *   FOR UPDATE SKIP LOCKED                            -- Skip rows locked by other workers
    * )
    * UPDATE outbox_events e
    * SET status = 'PROCESSING',                          -- Claim the events
    *     status_changed_at = NOW()
    * FROM claimed
    * WHERE e.id = claimed.id
    * RETURNING ...                                       -- Return claimed events
    * }}}
    *
    * == Concurrency Example ==
    * {{{
    * # Database state: Events [1, 2, 3, 4, 5, 6] all PENDING
    *
    * # Two workers call simultaneously:
    * Worker A: findAndClaimUnprocessed(limit=3)
    * Worker B: findAndClaimUnprocessed(limit=3)
    *
    * # Worker A locks and claims:
    * # → SELECT ... FOR UPDATE SKIP LOCKED → locks [1, 2, 3]
    * # → UPDATE [1, 2, 3] SET status='PROCESSING'
    * # → RETURNING [1, 2, 3]
    *
    * # Worker B (concurrent):
    * # → SELECT ... FOR UPDATE SKIP LOCKED → skips [1, 2, 3] (locked by A)
    * # → locks [4, 5, 6]
    * # → UPDATE [4, 5, 6] SET status='PROCESSING'
    * # → RETURNING [4, 5, 6]
    *
    * # Result: No overlap, each worker processes different events
    * }}}
    *
    * == Retry Scheduling ==
    * {{{
    * # Event 123 failed, scheduled for retry
    * # next_retry_at = T+5s
    *
    * # At T+2s: findAndClaimUnprocessed()
    * # → WHERE next_retry_at <= NOW() is FALSE (T+2s < T+5s)
    * # → Event 123 NOT returned (not ready yet)
    *
    * # At T+6s: findAndClaimUnprocessed()
    * # → WHERE next_retry_at <= NOW() is TRUE (T+6s > T+5s)
    * # → Event 123 returned (ready for retry)
    * }}}
    *
    * == FIFO Ordering ==
    * Events processed in creation order (ORDER BY created_at).
    * Ensures older events don't get starved by newer events.
    *
    * @param limit Maximum number of events to claim (default: 100)
    * @return Sequence of claimed events with status=PROCESSING
    */
  def findAndClaimUnprocessed(limit: Int = 100): DBIO[Seq[OutboxEvent]] = {
    val pendingStatus    = EventStatus.Pending.value
    val processingStatus = EventStatus.Processing.value
    sql"""
      WITH claimed AS (
        SELECT id
        FROM outbox_events
        WHERE status = $pendingStatus
          AND (next_retry_at IS NULL OR next_retry_at <= NOW())
        ORDER BY created_at
        LIMIT $limit
        FOR UPDATE SKIP LOCKED
      )
      UPDATE outbox_events e
      SET status = $processingStatus,
          status_changed_at = NOW()
      FROM claimed
      WHERE e.id = claimed.id
      RETURNING e.id, e.aggregate_id, e.event_type, e.payloads,
      e.created_at, e.status, e.status_changed_at, e.retry_count,
      e.last_error, e.moved_to_dlq, e.idempotency_key, e.next_retry_at
    """.as[OutboxEvent]
  }

  /** Marks an event as successfully processed.
    *
    * Called by OutboxProcessor after all fanout destinations have been successfully published.
    * Sets status=PROCESSED and records the completion time.
    *
    * == Example ==
    * {{{
    * # Event 456: OrderCreated successfully published to all destinations
    * # (inventory ✓, fraudCheck ✓, shipping ✓, billing ✓)
    *
    * markProcessed(456)
    * # → UPDATE outbox_events SET status='PROCESSED', status_changed_at=NOW() WHERE id=456
    * # → Event no longer picked up by OutboxProcessor
    * }}}
    *
    * @param id The event ID
    * @return DBIO action that returns number of rows updated (1 if successful)
    */
  def markProcessed(id: Long): DBIO[Int] =
    outbox
      .filter(_.id === id)
      .map(e => (e.status, e.statusChangedAt))
      .update((EventStatus.Processed, Some(Instant.now())))

  /** Increments retry count and schedules event for retry with exponential backoff.
    *
    * Called by OutboxProcessor when event publishing fails with a retryable error.
    * Uses optimistic locking (checks currentRetryCount) to prevent race conditions.
    *
    * == Example: Rate Limit (429) ==
    * {{{
    * # Attempt 1 fails: fraudCheck API returns 429 with Retry-After: 30 seconds
    * incrementRetryCount(id=456, currentRetryCount=0, error="HTTP 429", nextRetryAt=now+30s)
    *
    * # SQL:
    * UPDATE outbox_events
    * SET retry_count = 1,
    *     last_error = 'HTTP 429',
    *     status = 'PENDING',
    *     next_retry_at = NOW() + INTERVAL '30 seconds',
    *     status_changed_at = NOW()
    * WHERE id = 456
    *   AND retry_count = 0        -- Optimistic locking: only update if still at retry 0
    *   AND status = 'PROCESSING'  -- Sanity check: must be currently processing
    *
    * # Result: Event goes back to PENDING queue, will be picked up after 30 seconds
    * }}}
    *
    * == Optimistic Locking ==
    * Prevents race condition where two workers try to update the same event:
    * {{{
    * # Both Worker A and B try to increment retry count
    * Worker A: incrementRetryCount(456, currentRetryCount=0, ...)
    * Worker B: incrementRetryCount(456, currentRetryCount=0, ...)  # Concurrent
    *
    * # Worker A: WHERE retry_count = 0 → TRUE → UPDATE succeeds, retry_count now 1
    * # Worker B: WHERE retry_count = 0 → FALSE (now 1) → UPDATE fails, returns 0 rows
    * # Result: Only one increment, no data corruption
    * }}}
    *
    * @param id The event ID
    * @param currentRetryCount The current retry count (for optimistic locking)
    * @param error The error message from the failed attempt
    * @param nextRetryAt When this event should be retried (from Retry-After header or exponential backoff)
    * @return DBIO action that returns number of rows updated (1 if successful, 0 if optimistic lock failed)
    */
  def incrementRetryCount(
      id: Long,
      currentRetryCount: Int,
      error: String,
      nextRetryAt: Instant
  ): DBIO[Int] = {
    val pendingStatus    = EventStatus.Pending.value
    val processingStatus = EventStatus.Processing.value
    val nextRetryCount   = currentRetryCount + 1
    val nextRetryTs      = java.sql.Timestamp.from(nextRetryAt)
    sqlu"""
      UPDATE outbox_events
      SET retry_count = $nextRetryCount,
          last_error = ${error.take(500)},
          status = $pendingStatus,
          next_retry_at = $nextRetryTs,
          status_changed_at = NOW()
      WHERE id = $id
        AND retry_count = $currentRetryCount
        AND status = $processingStatus
    """
  }

  /** Moves an event to the dead letter queue and triggers automatic Saga compensation.
    *
    * Called by OutboxProcessor when event exhausts all retry attempts. This triggers
    * the Saga compensation pattern to undo all previously successful operations.
    *
    * == Flow ==
    * {{{
    * # Event 456: OrderCreated failed at billing after 3 retries
    * # Forward calls: inventory ✓, fraudCheck ✓, shipping ✓, billing ✗
    *
    * moveToDLQ(event, reason="MAX_RETRIES_EXCEEDED", error="HTTP 503: Service Unavailable")
    *
    * # Transaction:
    * BEGIN
    *   # 1. Insert into dead_letter_events
    *   INSERT INTO dead_letter_events (
    *     original_event_id = 456,
    *     aggregate_id = '123',
    *     event_type = 'OrderCreated',
    *     status = 'PENDING',           -- Ready for DLQProcessor to compensate
    *     retry_count = 3,               -- How many times forward was tried
    *     revert_retry_count = 0,        -- Compensation hasn't started yet
    *     reason = 'MAX_RETRIES_EXCEEDED',
    *     last_error = 'HTTP 503: Service Unavailable'
    *   )
    *   # Returns: dlqId (e.g., 789)
    *
    *   # 2. Mark outbox event as processed with DLQ flag
    *   UPDATE outbox_events
    *   SET status = 'PROCESSED',        -- Won't be retried by OutboxProcessor
    *       moved_to_dlq = true,         -- Flag: this event triggered compensation
    *       last_error = 'HTTP 503: Service Unavailable',
    *       status_changed_at = NOW()
    *   WHERE id = 456
    * COMMIT
    *
    * # Next: DLQProcessor picks up event 789 and performs LIFO compensation:
    * # → Query aggregate_results ORDER BY fanout_order DESC
    * # → Returns: [shipping(2), fraudCheck(1), inventory(0)]
    * # → Compensate: shipping → inventory (fraudCheck skipped, billing failed)
    * }}}
    *
    * == Why PROCESSED with moved_to_dlq=true ==
    * We mark as PROCESSED (not FAILED) because:
    * - Prevents OutboxProcessor from retrying (exhausted retries)
    * - Distinguishes from successfully processed events (moved_to_dlq=false)
    * - Enables queries like "count successful events" (status=PROCESSED AND moved_to_dlq=false)
    *
    * @param event The exhausted outbox event
    * @param reason Why moved to DLQ (e.g., "MAX_RETRIES_EXCEEDED", "NON_RETRYABLE_ERROR")
    * @param error The final error message
    * @return DBIO action that returns the new DLQ event ID
    */
  def moveToDLQ(event: OutboxEvent, reason: String, error: String): DBIO[Long] =
    for {
      dlqId <- dlqRepo.insertFromOutboxEvent(event, reason, error)
      _ <- outbox
        .filter(_.id === event.id)
        .map(e => (e.status, e.statusChangedAt, e.lastError, e.movedToDlq))
        .update((EventStatus.Processed, Some(Instant.now()), Some(error.take(500)), true))
    } yield dlqId

  def setError(id: Long, error: String): DBIO[Int] =
    outbox
      .filter(_.id === id)
      .map(_.lastError)
      .update(Some(error.take(500)))

  def countPending: DBIO[Int] =
    outbox.filter(_.status === (EventStatus.Pending: EventStatus)).length.result

  def countProcessed: DBIO[Int] =
    outbox.filter(_.status === (EventStatus.Processed: EventStatus)).length.result

  def countSuccessfullyProcessed: DBIO[Int] =
    outbox
      .filter(e => e.status === (EventStatus.Processed: EventStatus) && e.movedToDlq === false)
      .length
      .result

  /** Resets stale PROCESSING events back to PENDING.
    *
    * Events can get stuck in PROCESSING status if:
    * - Worker crashes while processing
    * - Network partition prevents completion
    * - External API hangs indefinitely
    *
    * This method finds events that have been PROCESSING for longer than the timeout
    * and resets them to PENDING so they can be retried.
    *
    * @param timeoutMinutes How long an event can be PROCESSING before being considered stale
    * @return Number of events reset
    */
  def resetStaleProcessingEvents(timeoutMinutes: Int = 5): DBIO[Int] = {
    val timeoutSeconds   = timeoutMinutes * 60
    val pendingStatus    = EventStatus.Pending.value
    val processingStatus = EventStatus.Processing.value
    sqlu"""
      UPDATE outbox_events
      SET status = $pendingStatus,
          status_changed_at = NOW()
      WHERE status = $processingStatus
        AND status_changed_at IS NOT NULL
        AND status_changed_at < NOW() - ($timeoutSeconds || ' seconds')::INTERVAL
        AND retry_count < $maxRetries
    """
  }

  /** Counts how many events are currently stuck in PROCESSING state. */
  def countStaleProcessingEvents(timeoutMinutes: Int = 5): DBIO[Int] = {
    val timeoutSeconds   = timeoutMinutes * 60
    val processingStatus = EventStatus.Processing.value
    sql"""
      SELECT COUNT(*)
      FROM outbox_events
      WHERE status = $processingStatus
        AND status_changed_at IS NOT NULL
        AND status_changed_at < NOW() - ($timeoutSeconds || ' seconds')::INTERVAL
        AND retry_count < $maxRetries
    """.as[Int].head
  }

  /** Returns the next time when a pending event will be due for processing.
    *
    * This is used for intelligent scheduling - instead of polling continuously,
    * we can schedule the next check exactly when the earliest event becomes ready.
    *
    * @return None if no pending events exist, Some(timestamp) otherwise
    */
  def getNextDueTime: DBIO[Option[Instant]] = {
    given GetResult[Option[Instant]] = GetResult(r => r.nextTimestampOption().map(_.toInstant))
    sql"""
      SELECT MIN(coalesce(next_retry_at, created_at)) as next_due
      FROM outbox_events
      WHERE status = ${EventStatus.Pending.value}
    """.as[Option[Instant]].head
  }

  private given GetResult[OutboxEvent] = GetResult { r =>
    OutboxEvent(
      id              = r.nextLong(),
      aggregateId     = r.nextString(),
      eventType       = r.nextString(),
      payloads        = Json.parse(r.nextString()).as[Map[String, DestinationConfig]],
      createdAt       = r.nextTimestamp().toInstant,
      status          = EventStatus.fromString(r.nextString()),
      statusChangedAt = r.nextTimestampOption().map(_.toInstant),
      retryCount      = r.nextInt(),
      lastError       = r.nextStringOption(),
      movedToDlq      = r.nextBoolean(),
      idempotencyKey  = r.nextString(),
      nextRetryAt     = r.nextTimestampOption().map(_.toInstant)
    )
  }
}
