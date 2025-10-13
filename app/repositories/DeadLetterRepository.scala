package repositories

import com.google.inject.{ Inject, Singleton }
import models.{ DeadLetterEvent, OutboxEvent }
import slick.jdbc.PostgresProfile.api.*

import java.time.Instant
import scala.concurrent.ExecutionContext

class DeadLetterTable(tag: Tag) extends Table[DeadLetterEvent](tag, "dead_letter_events") {
  def * = (
    id,
    originalEventId,
    aggregateId,
    eventType,
    payload,
    createdAt,
    failedAt,
    retryCount,
    lastError,
    reason
  ).mapTo[DeadLetterEvent]

  def id              = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def originalEventId = column[Long]("original_event_id")
  def aggregateId     = column[String]("aggregate_id")
  def eventType       = column[String]("event_type")
  def payload         = column[String]("payload")
  def createdAt       = column[Instant]("created_at")
  def failedAt        = column[Instant]("failed_at")
  def retryCount      = column[Int]("retry_count")
  def lastError       = column[String]("last_error")
  def reason          = column[String]("reason")
}

@Singleton
class DeadLetterRepository @Inject() ()(using ec: ExecutionContext) {
  private val deadLetters = TableQuery[DeadLetterTable]

  def insertFromOutboxEvent(
      event: OutboxEvent,
      reason: String,
      error: String
  ): DBIO[Long] = insert(
    DeadLetterEvent(
      originalEventId = event.id,
      aggregateId     = event.aggregateId,
      eventType       = event.eventType,
      payload         = event.payload,
      createdAt       = event.createdAt,
      failedAt        = Instant.now(),
      retryCount      = event.retryCount,
      lastError       = error,
      reason          = reason
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
}
