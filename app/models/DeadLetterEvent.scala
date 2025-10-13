package models

import play.api.libs.json.{ Format, JsValue, Json }

import java.time.Instant

case class DeadLetterEvent(
    id: Long = 0L,
    originalEventId: Long,
    aggregateId: String,
    eventType: String,
    payloads: Map[String, DestinationConfig],
    createdAt: Instant,
    failedAt: Instant = Instant.now(),
    retryCount: Int,
    lastError: Option[String],
    reason: String,
    status: EventStatus              = EventStatus.Pending,
    statusChangedAt: Option[Instant] = None,
    revertRetryCount: Int            = 0,
    revertError: Option[String]      = None,
    nextRetryAt: Option[Instant]     = None
) extends RetryableEvent
    with AggregateEvent

object DeadLetterEvent {
  import EventStatus.given
  given Format[DeadLetterEvent] = Json.format[DeadLetterEvent]
}
