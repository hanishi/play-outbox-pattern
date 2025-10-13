package models

import play.api.libs.json.{ Format, Json }

import java.time.Instant

case class DeadLetterEvent(
    id: Long = 0L,
    originalEventId: Long,
    aggregateId: String,
    eventType: String,
    payload: String,
    createdAt: Instant,
    failedAt: Instant = Instant.now(),
    retryCount: Int,
    lastError: String,
    reason: String
)

object DeadLetterEvent {
  given Format[DeadLetterEvent] = Json.format[DeadLetterEvent]
}