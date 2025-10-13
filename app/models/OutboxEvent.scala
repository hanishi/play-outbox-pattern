package models

import play.api.libs.json.{ Format, Json }

import java.time.Instant

case class OutboxEvent(
    id: Long = 0L,
    aggregateId: String,
    eventType: String,
    payload: String,
    createdAt: Instant           = Instant.now(),
    processedAt: Option[Instant] = None,
    retryCount: Int              = 0,
    lastError: Option[String]    = None,
    movedToDlq: Boolean          = false
)

object OutboxEvent {
  given Format[OutboxEvent] = Json.format[OutboxEvent]
}
