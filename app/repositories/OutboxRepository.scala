package repositories
import com.google.inject.{ Inject, Singleton }
import models.{ EventStatus, OutboxEvent }
import slick.jdbc.GetResult
import slick.jdbc.PostgresProfile.api.*

import java.time.Instant
import scala.concurrent.ExecutionContext
class OutboxTable(tag: Tag) extends Table[OutboxEvent](tag, "outbox_events") {
  def * = (
    id,
    aggregateId,
    eventType,
    payload,
    createdAt,
    status,
    processedAt,
    retryCount,
    lastError,
    movedToDlq,
    idempotencyKey
  ).mapTo[OutboxEvent]

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def aggregateId = column[String]("aggregate_id")

  def eventType = column[String]("event_type")

  def payload = column[String]("payload")

  def createdAt = column[Instant]("created_at")

  def status = column[EventStatus]("status")

  def processedAt = column[Option[Instant]]("processed_at")

  def retryCount = column[Int]("retry_count")

  def lastError = column[Option[String]]("last_error")

  def movedToDlq = column[Boolean]("moved_to_dlq")

  def idempotencyKey = column[String]("idempotency_key")

  // Column type mapper for EventStatus
  given BaseColumnType[EventStatus] =
    MappedColumnType.base[EventStatus, String](
      status => status.value,
      str => EventStatus.fromString(str)
    )
}

@Singleton
class OutboxRepository @Inject() (
    dlqRepo: DeadLetterRepository,
    config: play.api.Configuration
)(using ec: ExecutionContext) {
  val outbox = TableQuery[OutboxTable]

  private val maxRetries = config.getOptional[Int]("outbox.maxRetries").getOrElse(2)

  def insert(event: OutboxEvent): DBIO[Long] = outbox returning outbox.map(_.id) += event

  /** Finds and claims pending events for processing.
    *
    * This method uses a status-based approach to prevent race conditions:
    * 1. SELECT events with status='PENDING' and retry_count < maxRetries
    * 2. Immediately UPDATE their status to 'PROCESSING' within the same transaction
    * 3. Return the claimed events for processing
    *
    * This prevents the race condition where multiple processors could grab the same event
    * because once an event is marked as PROCESSING, subsequent polls will skip it.
    *
    * DELIVERY SEMANTICS:
    * ===================
    * - At-least-once delivery guarantee (events processed at least once)
    * - No duplicate processing under normal conditions
    * - Stale PROCESSING events (e.g., from crashed workers) can be recovered
    *   by a separate cleanup job that resets status to PENDING after a timeout
    */
  def find(id: Long): DBIO[OutboxEvent] =
    sql"""
      SELECT id, aggregate_id, event_type, payload, created_at, status, processed_at, retry_count, last_error, moved_to_dlq, idempotency_key
      FROM outbox_events
      WHERE id = $id
    """.as[OutboxEvent].head

  /** Atomically claims and returns pending events for processing.
    *
    * Uses a CTE (Common Table Expression) to:
    * 1. Select pending events with FOR UPDATE SKIP LOCKED
    * 2. Update their status to PROCESSING
    * 3. Return the claimed events
    *
    * This happens atomically in a single database round-trip.
    */
  def findAndClaimUnprocessed(limit: Int = 100): DBIO[Seq[OutboxEvent]] =
    sql"""
      WITH claimed AS (
        SELECT id
        FROM outbox_events
        WHERE status = 'PENDING'
          AND retry_count < $maxRetries
        ORDER BY created_at
        LIMIT $limit
        FOR UPDATE SKIP LOCKED
      )
      UPDATE outbox_events e
      SET status = 'PROCESSING'
      FROM claimed
      WHERE e.id = claimed.id
      RETURNING e.id, e.aggregate_id, e.event_type, e.payload, e.created_at, e.status, e.processed_at, e.retry_count, e.last_error, e.moved_to_dlq, e.idempotency_key
    """.as[OutboxEvent]

  /** Marks an event as successfully processed. */
  def markProcessed(id: Long): DBIO[Int] =
    sqlu"""
      UPDATE outbox_events
      SET status = 'PROCESSED',
          processed_at = NOW()
      WHERE id = $id
    """

  /** Increments retry count and sets status back to PENDING for retry. */
  def incrementRetryCount(id: Long, error: String): DBIO[Int] =
    sqlu"""
      UPDATE outbox_events
      SET retry_count = retry_count + 1,
          last_error = ${error.take(500)},
          status = 'PENDING'
      WHERE id = $id
    """

  /** Moves an event to the dead letter queue. */
  def moveToDLQ(event: OutboxEvent, reason: String, error: String): DBIO[Long] =
    for {
      dlqId <- dlqRepo.insertFromOutboxEvent(event, reason, error)
      _ <- sqlu"""
        UPDATE outbox_events
        SET status = 'PROCESSED',
            processed_at = NOW(),
            last_error = ${error.take(500)},
            moved_to_dlq = TRUE
        WHERE id = ${event.id}
      """
    } yield dlqId

  def setError(id: Long, error: String): DBIO[Int] =
    outbox
      .filter(_.id === id)
      .map(_.lastError)
      .update(Some(error.take(500)))

  def countPending: DBIO[Int] =
    outbox.filter(_.processedAt.isEmpty).length.result

  def countProcessed: DBIO[Int] =
    outbox.filter(_.processedAt.isDefined).length.result

  def countSuccessfullyProcessed: DBIO[Int] =
    outbox.filter(e => e.processedAt.isDefined && !e.movedToDlq).length.result

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
    val timeoutSeconds = timeoutMinutes * 60
    sqlu"""
      UPDATE outbox_events
      SET status = 'PENDING'
      WHERE status = 'PROCESSING'
        AND created_at < NOW() - ($timeoutSeconds || ' seconds')::INTERVAL
        AND retry_count < $maxRetries
    """
  }

  /** Counts how many events are currently stuck in PROCESSING state. */
  def countStaleProcessingEvents(timeoutMinutes: Int = 5): DBIO[Int] = {
    val timeoutSeconds = timeoutMinutes * 60
    sql"""
      SELECT COUNT(*)
      FROM outbox_events
      WHERE status = 'PROCESSING'
        AND created_at < NOW() - ($timeoutSeconds || ' seconds')::INTERVAL
        AND retry_count < $maxRetries
    """.as[Int].head
  }

  private given GetResult[OutboxEvent] = GetResult { r =>
    OutboxEvent(
      id             = r.nextLong(),
      aggregateId    = r.nextString(),
      eventType      = r.nextString(),
      payload        = r.nextString(),
      createdAt      = r.nextTimestamp().toInstant,
      status         = EventStatus.fromString(r.nextString()),
      processedAt    = r.nextTimestampOption().map(_.toInstant),
      retryCount     = r.nextInt(),
      lastError      = r.nextStringOption(),
      movedToDlq     = r.nextBoolean(),
      idempotencyKey = r.nextString()
    )
  }
}
