package models

import play.api.libs.json.*

import java.time.Instant

enum EventStatus(val value: String):
  case Pending extends EventStatus("PENDING")
  case Processing extends EventStatus("PROCESSING")
  case Processed extends EventStatus("PROCESSED")
  case Failed extends EventStatus("FAILED")

object EventStatus {
  private val eventStatusReads: Reads[EventStatus] = Reads {
    case JsString(s) => JsSuccess(fromString(s))
    case _ => JsError("Expected string for EventStatus")
  }
  private val eventStatusWrites: Writes[EventStatus] = Writes { status =>
    JsString(status.value)
  }

  def fromString(s: String): EventStatus = s.toUpperCase match {
    case "PENDING" => Pending
    case "PROCESSING" => Processing
    case "PROCESSED" => Processed
    case "FAILED" => Failed
    case _ => Pending // default fallback
  }

  given Format[EventStatus] = Format(eventStatusReads, eventStatusWrites)
}

case class OutboxEvent(
    id: Long = 0L,
    aggregateId: String,
    eventType: String,
    payload: String,
    createdAt: Instant           = Instant.now(),
    status: EventStatus          = EventStatus.Pending,
    processedAt: Option[Instant] = None,
    retryCount: Int              = 0,
    lastError: Option[String]    = None,
    movedToDlq: Boolean          = false,
    idempotencyKey: String       = "" // Generated as {aggregateId}:{eventType}
) {

  /** Helper to generate idempotency key if not provided */
  def withIdempotencyKey: OutboxEvent =
    if (idempotencyKey.nonEmpty) this
    else copy(idempotencyKey = s"$aggregateId:$eventType")
}

object OutboxEvent {
  import EventStatus.given
  given Format[OutboxEvent] = Json.format[OutboxEvent]
}
