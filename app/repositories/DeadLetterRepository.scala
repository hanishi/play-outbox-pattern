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
      lastError       = Some(error), // Wrap in Some()
      reason          = reason,
      status          = EventStatus.Pending,
      statusChangedAt = Some(Instant.now())
    )
  )

  private def insert(dlqEvent: DeadLetterEvent): DBIO[Long] =
    deadLetters returning deadLetters.map(_.id) += dlqEvent

  def findByOriginalEventId(originalEventId: Long): DBIO[Option[DeadLetterEvent]] =
    deadLetters.filter(_.originalEventId === originalEventId).result.headOption

  def list(limit: Int = 50, offset: Int = 0): DBIO[Seq[DeadLetterEvent]] =
    deadLetters
      .sortBy(_.failedAt.desc)
      .drop(offset)
      .take(limit)
      .result

  def countByReason(reason: String): DBIO[Int] =
    deadLetters.filter(_.reason === reason).length.result

  def countAll: DBIO[Int] =
    deadLetters.length.result

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
    */
  def getNextDueTime: DBIO[Option[Instant]] = {
    given GetResult[Option[Instant]] = GetResult(r => r.nextTimestampOption().map(_.toInstant))
    val pendingStatus                = EventStatus.Pending.value
    sql"""
      SELECT MIN(coalesce(next_retry_at, failed_at)) as next_due
      FROM dead_letter_events
      WHERE status = $pendingStatus
        AND revert_retry_count <= $maxRevertRetries
    """.as[Option[Instant]].head
  }
}
