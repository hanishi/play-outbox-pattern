package repositories

import com.google.inject.{ Inject, Singleton }
import models.{ DeadLetterEvent, DestinationConfig, OutboxEvent }
import repositories.OutboxEventsPostgresProfile.api.*
import slick.jdbc.GetResult

import java.time.Instant
import scala.concurrent.ExecutionContext

class DeadLetterTable(tag: Tag) extends Table[DeadLetterEvent](tag, "dead_letter_events") {
  import CommonMappers.given
  import models.EventStatus

  def * = (
    id,
    originalEventId,
    aggregateId,
    eventType,
    payloads,
    createdAt,
    failedAt,
    retryCount,
    lastError,
    reason,
    status,
    statusChangedAt,
    revertRetryCount,
    revertError,
    nextRetryAt
  ).mapTo[DeadLetterEvent]

  def id               = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def originalEventId  = column[Long]("original_event_id")
  def aggregateId      = column[String]("aggregate_id")
  def eventType        = column[String]("event_type")
  def payloads         = column[Map[String, DestinationConfig]]("payloads")
  def createdAt        = column[Instant]("created_at")
  def failedAt         = column[Instant]("failed_at")
  def retryCount       = column[Int]("retry_count")
  def lastError        = column[Option[String]]("last_error")
  def reason           = column[String]("reason")
  def status           = column[EventStatus]("status")
  def statusChangedAt  = column[Option[Instant]]("status_changed_at")
  def revertRetryCount = column[Int]("revert_retry_count")
  def revertError      = column[Option[String]]("revert_error")
  def nextRetryAt      = column[Option[Instant]]("next_retry_at")
}

/** Repository for managing dead letter queue (DLQ) events and automatic compensation.
  *
  * == Purpose ==
  * When forward events exhaust retries (e.g., billing fails after 3 attempts), they move to the DLQ
  * and automatically trigger Saga compensation to undo previously successful operations.
  *
  * This repository manages:
  * 1. '''DLQ Storage''': Stores failed forward events with original payload and error details
  * 2. '''Compensation Retry Logic''': Retries revert operations with exponential backoff
  * 3. '''Status Tracking''': PENDING → PROCESSING → PROCESSED (or FAILED after max retries)
  * 4. '''Optimistic Locking''': Prevents duplicate compensation via FOR UPDATE SKIP LOCKED
  *
  * == Table Structure ==
  * {{{
  * CREATE TABLE dead_letter_events (
  *   id BIGSERIAL PRIMARY KEY,
  *   original_event_id BIGINT NOT NULL,       -- Reference to original outbox_events.id
  *   aggregate_id VARCHAR NOT NULL,           -- e.g., "123" (order ID)
  *   event_type VARCHAR NOT NULL,             -- e.g., "OrderCreated"
  *   payloads JSONB NOT NULL,                 -- Original event payloads (for manual replay)
  *   created_at TIMESTAMP NOT NULL,           -- When original event was created
  *   failed_at TIMESTAMP NOT NULL,            -- When event moved to DLQ
  *   retry_count INT NOT NULL,                -- Forward retry count before DLQ (e.g., 3)
  *   last_error TEXT,                         -- Last forward error (e.g., "HTTP 503")
  *   reason VARCHAR NOT NULL,                 -- Why moved to DLQ (e.g., "MAX_RETRIES_EXCEEDED")
  *   status VARCHAR NOT NULL,                 -- PENDING, PROCESSING, PROCESSED, FAILED
  *   status_changed_at TIMESTAMP,             -- Last status change time
  *   revert_retry_count INT NOT NULL,         -- Revert retry attempts (0-5)
  *   revert_error TEXT,                       -- Last revert error if any
  *   next_retry_at TIMESTAMP                  -- When to retry compensation (NULL = ready now)
  * );
  * }}}
  *
  * == Lifecycle ==
  *
  * '''1. Forward Event Fails:'''
  * {{{
  * # OrderCreated event: inventory → fraudCheck → shipping → billing
  * # billing fails after 3 retries (configured max)
  * # OutboxProcessor moves event to DLQ:
  *
  * INSERT INTO dead_letter_events (
  *   original_event_id = 456,
  *   aggregate_id = "123",
  *   event_type = "OrderCreated",
  *   payloads = {...},
  *   failed_at = NOW(),
  *   retry_count = 3,
  *   last_error = "HTTP 503: Service Unavailable",
  *   reason = "MAX_RETRIES_EXCEEDED",
  *   status = "PENDING",
  *   revert_retry_count = 0
  * );
  * }}}
  *
  * '''2. DLQProcessor Claims and Processes:'''
  * {{{
  * # findAndClaimPendingForRevert() claims event:
  * UPDATE dead_letter_events
  * SET status = 'PROCESSING', status_changed_at = NOW()
  * WHERE id = 789 AND status = 'PENDING'
  * FOR UPDATE SKIP LOCKED;  -- Prevents duplicate processing
  *
  * # DLQProcessor performs compensation:
  * # Query aggregate_results ORDER BY fanout_order DESC
  * # Returns: [shipping(2), fraudCheck(1), inventory(0)]  -- LIFO order
  * # Reverts: shipping → inventory (fraudCheck skipped, no revert config)
  * }}}
  *
  * '''3. Compensation Success:'''
  * {{{
  * # All reverts succeeded
  * markProcessed(789);
  *
  * UPDATE dead_letter_events
  * SET status = 'PROCESSED', status_changed_at = NOW()
  * WHERE id = 789;
  * }}}
  *
  * '''4. Compensation Failure (with retry):'''
  * {{{
  * # Inventory revert fails (503)
  * incrementRevertRetryCount(789, currentRetryCount=0, error="HTTP 503", nextRetryAt=now+2s);
  *
  * UPDATE dead_letter_events
  * SET revert_retry_count = 1,
  *     revert_error = "HTTP 503",
  *     status = 'PENDING',
  *     next_retry_at = NOW() + INTERVAL '2 seconds'
  * WHERE id = 789 AND revert_retry_count = 0;
  *
  * # DLQProcessor will retry after 2 seconds (exponential backoff: 2^1, 2^2, 2^3...)
  * }}}
  *
  * '''5. Permanent Failure (max retries):'''
  * {{{
  * # After 5 revert attempts, mark as FAILED
  * markRevertFailed(789, "HTTP 503: Service Unavailable");
  *
  * UPDATE dead_letter_events
  * SET status = 'FAILED',
  *     revert_error = "HTTP 503: Service Unavailable"
  * WHERE id = 789;
  *
  * # Manual intervention required (alert ops team)
  * }}}
  *
  * == Key Queries ==
  *
  * - '''findAndClaimPendingForRevert''': Claims pending DLQ events for compensation (FOR UPDATE SKIP LOCKED)
  * - '''incrementRevertRetryCount''': Marks revert failure and schedules retry (exponential backoff)
  * - '''markProcessed''': Marks compensation as successfully completed
  * - '''markRevertFailed''': Marks compensation as permanently failed after max retries
  * - '''getNextDueTime''': Gets earliest retry time for DLQ scheduler
  */
@Singleton
class DeadLetterRepository @Inject() (config: play.api.Configuration)(using ec: ExecutionContext) {
  import models.EventStatus
  private val deadLetters      = TableQuery[DeadLetterTable]
  private val maxRevertRetries = config.getOptional[Int]("outbox.maxRetries").getOrElse(5)

  given GetResult[DeadLetterEvent] = GetResult { r =>
    DeadLetterEvent(
      id              = r.nextLong(),
      originalEventId = r.nextLong(),
      aggregateId     = r.nextString(),
      eventType       = r.nextString(),
      payloads        = {
        val jsValue = r.nextString()
        play.api.libs.json.Json.parse(jsValue).as[Map[String, models.DestinationConfig]]
      },
      createdAt        = r.nextTimestamp().toInstant,
      failedAt         = r.nextTimestamp().toInstant,
      retryCount       = r.nextInt(),
      lastError        = r.nextStringOption(),
      reason           = r.nextString(),
      status           = EventStatus.fromString(r.nextString()),
      statusChangedAt  = Option(r.nextTimestampOption()).flatten.map(_.toInstant),
      revertRetryCount = r.nextInt(),
      revertError      = r.nextStringOption(),
      nextRetryAt      = Option(r.nextTimestampOption()).flatten.map(_.toInstant)
    )
  }

  /** Moves a failed outbox event to the DLQ, triggering automatic compensation.
    *
    * Called by OutboxProcessor when a forward event exhausts all retries.
    * Creates a DLQ entry with status=PENDING, which will be picked up by DLQProcessor
    * to perform Saga compensation (undo previously successful operations).
    *
    * == Example ==
    * {{{
    * # Scenario: OrderCreated event fails at billing after 3 retries
    * # Forward flow was: inventory ✓ → fraudCheck ✓ → shipping ✓ → billing ✗
    *
    * insertFromOutboxEvent(
    *   event = OutboxEvent(
    *     id = 456,
    *     aggregateId = "123",
    *     eventType = "OrderCreated",
    *     payloads = Map(
    *       "inventory" -> DestinationConfig(...),
    *       "fraudCheck" -> DestinationConfig(...),
    *       "shipping" -> DestinationConfig(...),
    *       "billing" -> DestinationConfig(...)
    *     ),
    *     retryCount = 3
    *   ),
    *   reason = "MAX_RETRIES_EXCEEDED",
    *   error = "HTTP 503: Service Unavailable"
    * )
    *
    * # Inserts DLQ event:
    * INSERT INTO dead_letter_events (
    *   original_event_id = 456,
    *   aggregate_id = "123",
    *   event_type = "OrderCreated",
    *   payloads = {...},
    *   failed_at = NOW(),
    *   retry_count = 3,
    *   last_error = "HTTP 503: Service Unavailable",
    *   reason = "MAX_RETRIES_EXCEEDED",
    *   status = "PENDING",
    *   revert_retry_count = 0
    * );
    *
    * # Next step: DLQProcessor picks this up and reverts in LIFO order:
    * # → shipping (cancel shipment)
    * # → inventory (release reservation)
    * # (fraudCheck skipped - read-only, no revert config)
    * }}}
    *
    * @param event The original failed outbox event
    * @param reason Why the event was moved to DLQ (e.g., "MAX_RETRIES_EXCEEDED", "NON_RETRYABLE_ERROR")
    * @param error The last error message from the failed attempt
    * @return DBIO action that returns the new DLQ event ID
    */
  def insertFromOutboxEvent(
      event: OutboxEvent,
      reason: String,
      error: String
  ): DBIO[Long] = insert(
    DeadLetterEvent(
      originalEventId = event.id,
      aggregateId     = event.aggregateId,
      eventType       = event.eventType,
      payloads        = event.payloads,
      createdAt       = event.createdAt,
      failedAt        = Instant.now(),
      retryCount      = event.retryCount,
      lastError       = Some(error),
      reason          = reason,
      status          = EventStatus.Pending,
      statusChangedAt = Some(Instant.now())
    )
  )

  private def insert(dlqEvent: DeadLetterEvent): DBIO[Long] =
    deadLetters returning deadLetters.map(_.id) += dlqEvent

  def countByReason(reason: String): DBIO[Int] =
    deadLetters.filter(_.reason === reason).length.result

  def countAll: DBIO[Int] =
    deadLetters.length.result

  /** Atomically claims pending DLQ events for compensation processing.
    *
    * Uses FOR UPDATE SKIP LOCKED to prevent duplicate compensation when multiple DLQProcessor
    * instances are running. Only claims events that are ready to be processed (due now).
    *
    * == Query Logic ==
    * {{{
    * # Filters
    * WHERE status = 'PENDING'                          -- Not already being processed
    *   AND revert_retry_count <= maxRevertRetries      -- Haven't exceeded retry limit (default: 5)
    *   AND (next_retry_at IS NULL OR next_retry_at <= NOW())  -- Ready now (not waiting for retry delay)
    *   AND event_type NOT LIKE '!%'                    -- Exclude manual revert events (!OrderCreated)
    * ORDER BY failed_at                                -- FIFO: oldest failures first
    * FOR UPDATE SKIP LOCKED                            -- Skip rows locked by other processors
    * }}}
    *
    * == Concurrency Safety ==
    * {{{
    * # Scenario: 3 DLQProcessor instances running concurrently
    *
    * T1: Processor A calls findAndClaimPendingForRevert(limit=100)
    *     → Locks events [1, 2, 3]
    *     → Updates status to PROCESSING
    *     → Returns [1, 2, 3]
    *
    * T2: Processor B calls findAndClaimPendingForRevert(limit=100) (concurrent with T1)
    *     → FOR UPDATE SKIP LOCKED skips events [1, 2, 3] (already locked by A)
    *     → Locks events [4, 5, 6]
    *     → Returns [4, 5, 6]
    *
    * T3: Processor C calls findAndClaimPendingForRevert(limit=100) (concurrent with T1, T2)
    *     → Skips [1, 2, 3, 4, 5, 6]
    *     → Locks events [7, 8]
    *     → Returns [7, 8]
    *
    * # Result: No duplicate compensation, all 3 processors work on different events
    * }}}
    *
    * == Retry Scheduling ==
    * {{{
    * # Scenario: Event failed compensation, scheduled for retry
    *
    * # Initial DLQ entry
    * id=789, status=PENDING, revert_retry_count=0, next_retry_at=NULL
    * # → findAndClaimPendingForRevert returns this event immediately
    *
    * # After first revert failure
    * incrementRevertRetryCount(789, 0, "HTTP 503", now+2s)
    * # → id=789, status=PENDING, revert_retry_count=1, next_retry_at=T+2s
    *
    * # At T+1s: findAndClaimPendingForRevert
    * # → Skips event 789 (next_retry_at > NOW)
    *
    * # At T+3s: findAndClaimPendingForRevert
    * # → Returns event 789 (next_retry_at <= NOW, ready for retry)
    * }}}
    *
    * == Exclusions ==
    * - '''Manual revert events''' (!OrderCreated): Excluded because they're user-initiated cancellations,
    *   not automatic compensation. They're handled by HttpEventPublisher directly.
    * - '''Exceeded retry limit''': Events with revert_retry_count > maxRevertRetries are skipped
    *   (should be marked as FAILED by markRevertFailed)
    *
    * @param limit Maximum number of events to claim (default: 100)
    * @return Sequence of claimed DLQ events with status=PROCESSING
    */
  def findAndClaimPendingForRevert(limit: Int = 100): DBIO[Seq[DeadLetterEvent]] = {
    val pendingStatus    = EventStatus.Pending.value
    val processingStatus = EventStatus.Processing.value
    sql"""
      WITH claimed AS (
        SELECT id
        FROM dead_letter_events
        WHERE status = $pendingStatus
          AND revert_retry_count <= $maxRevertRetries
          AND (next_retry_at IS NULL OR next_retry_at <= NOW())
          AND event_type NOT LIKE '!%'
        ORDER BY failed_at
        LIMIT $limit
        FOR UPDATE SKIP LOCKED
      )
      UPDATE dead_letter_events e
      SET status = $processingStatus,
          status_changed_at = NOW()
      FROM claimed
      WHERE e.id = claimed.id
      RETURNING e.id, e.original_event_id, e.aggregate_id, e.event_type, e.payloads::text,
      e.created_at, e.failed_at, e.retry_count, e.last_error, e.reason,
      e.status, e.status_changed_at, e.revert_retry_count, e.revert_error, e.next_retry_at
    """.as[DeadLetterEvent]
  }

  /** Marks a DLQ event as successfully processed after revert. */
  def markProcessed(id: Long): DBIO[Int] = {
    val processedStatus = EventStatus.Processed.value
    sqlu"""
      UPDATE dead_letter_events
      SET status = $processedStatus,
          status_changed_at = NOW()
      WHERE id = $id
    """
  }

  /** Increments revert retry count and sets status back to PENDING for retry.
    * @param id The DLQ event ID
    * @param currentRetryCount The current revert retry count (for optimistic locking)
    * @param error The error message
    * @param nextRetryAt When this event should be retried
    */
  def incrementRevertRetryCount(
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
      UPDATE dead_letter_events
      SET revert_retry_count = $nextRetryCount,
          revert_error = ${error.take(500)},
          status = $pendingStatus,
          next_retry_at = $nextRetryTs,
          status_changed_at = NOW()
      WHERE id = $id
        AND revert_retry_count = $currentRetryCount
        AND revert_retry_count < $maxRevertRetries
        AND status = $processingStatus
    """
  }

  /** Marks a DLQ event as permanently failed after max retries.
    * No "DLQ for DLQ" - just mark as FAILED status.
    */
  def markRevertFailed(id: Long, error: String): DBIO[Int] = {
    val failedStatus = EventStatus.Failed.value
    sqlu"""
      UPDATE dead_letter_events
      SET status = $failedStatus,
          status_changed_at = NOW(),
          revert_error = ${error.take(500)}
      WHERE id = $id
    """
  }

  def setRevertError(id: Long, error: String): DBIO[Int] =
    deadLetters
      .filter(_.id === id)
      .map(_.revertError)
      .update(Some(error.take(500)))

  def countPending: DBIO[Int] = {
    val pendingStatus = EventStatus.Pending.value
    sql"""SELECT COUNT(*) FROM dead_letter_events WHERE status = $pendingStatus"""
      .as[Int]
      .map(_.head)
  }

  def countProcessed: DBIO[Int] = {
    val processedStatus = EventStatus.Processed.value
    sql"""SELECT COUNT(*) FROM dead_letter_events WHERE status = $processedStatus"""
      .as[Int]
      .map(_.head)
  }

  def countFailed: DBIO[Int] = {
    val failedStatus = EventStatus.Failed.value
    sql"""SELECT COUNT(*) FROM dead_letter_events WHERE status = $failedStatus"""
      .as[Int]
      .map(_.head)
  }

  /** Get the next due time for DLQ events that need revert retry.
    * Returns None if no pending events, otherwise returns the earliest due time.
    * Excludes revert events (event_type LIKE '!%') since they're not processed by DLQProcessor.
    */
  def getNextDueTime: DBIO[Option[Instant]] = {
    given GetResult[Option[Instant]] = GetResult(r => r.nextTimestampOption().map(_.toInstant))
    val pendingStatus                = EventStatus.Pending.value
    sql"""
      SELECT MIN(coalesce(next_retry_at, failed_at)) as next_due
      FROM dead_letter_events
      WHERE status = $pendingStatus
        AND revert_retry_count <= $maxRevertRetries
        AND event_type NOT LIKE '!%'
    """.as[Option[Instant]].head
  }
}
