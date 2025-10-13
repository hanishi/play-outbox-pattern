package repositories
import com.google.inject.{ Inject, Singleton }
import models.OutboxEvent
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
    processedAt,
    retryCount,
    lastError,
    movedToDlq
  ).mapTo[OutboxEvent]

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def aggregateId = column[String]("aggregate_id")

  def eventType = column[String]("event_type")

  def payload = column[String]("payload")

  def createdAt = column[Instant]("created_at")

  def processedAt = column[Option[Instant]]("processed_at")

  def retryCount = column[Int]("retry_count")

  def lastError = column[Option[String]]("last_error")

  def movedToDlq = column[Boolean]("moved_to_dlq")
}

@Singleton
class OutboxRepository @Inject() (dlqRepo: DeadLetterRepository)(using ec: ExecutionContext) {
  val outbox = TableQuery[OutboxTable]

  def insert(event: OutboxEvent): DBIO[Long] = outbox returning outbox.map(_.id) += event

  /** Finds unprocessed events with row-level locking.
    *
    * TRANSACTION BOUNDARY & LOCKING BEHAVIOR:
    * ========================================
    * FOR UPDATE SKIP LOCKED provides row locking ONLY within the transaction.
    * Once this query completes, the transaction closes and locks are released.
    *
    * RACE CONDITION SCENARIO:
    * ========================
    * 1. Processor A: Fetches event ID=123 at T0 (locks acquired, then immediately released)
    * 2. Processor A: Publishing to external API (slow, takes 30 seconds)
    * 3. Processor B: Polls at T0+5s, sees event ID=123 still has processed_at=NULL
    * 4. Processor B: Can fetch the SAME event ID=123 again!
    * 5. Both processors now publish the same event
    *
    * WHY THIS HAPPENS:
    * =================
    * - Event stays in "unprocessed" state (processed_at=NULL) until markProcessed completes
    * - If processing takes longer than the poll interval, another processor can grab it
    * - This is especially likely if: external API is slow, poll interval is short, or failures occur
    *
    * DELIVERY SEMANTICS:
    * ===================
    * This implementation provides **at-least-once** delivery:
    * - Events are guaranteed to be processed AT LEAST once
    * - Events MAY be processed MORE THAN once (rare, but possible)
    * - Your EventPublisher MUST be idempotent to handle duplicates
    *
    * CURRENT APPROACH RATIONALE:
    * ===========================
    * At-least-once delivery is standard for outbox patterns because:
    * - Simpler implementation (no extra status column or claim logic)
    * - External systems should be idempotent anyway (best practice)
    * - Race condition is rare in practice if processing is reasonably fast
    */
  def find(id: Long): DBIO[OutboxEvent] =
    sql"""
      SELECT id, aggregate_id, event_type, payload, created_at, processed_at, retry_count, last_error, moved_to_dlq
      FROM outbox_events
      WHERE id = $id
    """.as[OutboxEvent].head

  def findUnprocessed(limit: Int = 100): DBIO[Seq[OutboxEvent]] =
    sql"""
      SELECT id, aggregate_id, event_type, payload, created_at, processed_at, retry_count, last_error, moved_to_dlq
      FROM outbox_events
      WHERE processed_at IS NULL AND retry_count < 3
      ORDER BY created_at
      LIMIT $limit
      FOR UPDATE SKIP LOCKED
    """.as[OutboxEvent]
  

  def markProcessed(id: Long): DBIO[Int] =
    outbox
      .filter(_.id === id)
      .map(_.processedAt)
      .update(Some(Instant.now()))

  def incrementRetryCount(id: Long, error: String): DBIO[Int] =
    sqlu"""
      UPDATE outbox_events
      SET retry_count = retry_count + 1,
          last_error = ${error.take(500)}
      WHERE id = $id
    """

  def moveToDLQ(event: OutboxEvent, reason: String, error: String): DBIO[Long] =
    for {
      dlqId <- dlqRepo.insertFromOutboxEvent(event, reason, error)
      _ <- outbox
        .filter(_.id === event.id)
        .map(e => (e.processedAt, e.lastError, e.movedToDlq))
        .update((Some(Instant.now()), Some(error.take(500)), true))
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

  private given GetResult[OutboxEvent] = GetResult { r =>
    OutboxEvent(
      id          = r.nextLong(),
      aggregateId = r.nextString(),
      eventType   = r.nextString(),
      payload     = r.nextString(),
      createdAt   = r.nextTimestamp().toInstant,
      processedAt = r.nextTimestampOption().map(_.toInstant),
      retryCount  = r.nextInt(),
      lastError   = r.nextStringOption(),
      movedToDlq  = r.nextBoolean()
    )
  }
}
