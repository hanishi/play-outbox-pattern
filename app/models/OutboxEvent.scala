package models

import play.api.libs.json.*

import java.time.Instant

case class DestinationConfig(
    payload: Option[JsValue],
    pathParams: Map[String, String] = Map.empty
)

object DestinationConfig {
  given Format[DestinationConfig] = Json.format[DestinationConfig]
}

case class RoutingContext(destinationResponses: Map[String, JsValue] = Map.empty) {
  def withResponse(destination: String, response: JsValue): RoutingContext =
    copy(destinationResponses = destinationResponses + (destination -> response))
  def getResponse(destination: String): Option[JsValue] = destinationResponses.get(destination)
}

object RoutingContext {
  given Format[RoutingContext] = Json.format[RoutingContext]
}

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
    payloads: Map[String, DestinationConfig], // Destination-specific payloads with path params
    createdAt: Instant               = Instant.now(),
    status: EventStatus              = EventStatus.Pending,
    statusChangedAt: Option[Instant] =
      None, // When status last changed (for stale detection and completion time)
    retryCount: Int              = 0,
    lastError: Option[String]    = None,
    movedToDlq: Boolean          = false,
    idempotencyKey: String       = "", // Generated as {aggregateId}:{eventType}
    nextRetryAt: Option[Instant] = None // When this event should be retried (None = ready now)
) extends RetryableEvent
    with AggregateEvent {

  /** Helper to generate idempotency key if not provided */
  inline def withIdempotencyKey: OutboxEvent =
    if idempotencyKey.nonEmpty then this
    else copy(idempotencyKey = s"$aggregateId:$eventType")
}

object OutboxEvent:
  given outboxEventFormat(
      using Format[EventStatus],
      Format[RoutingContext],
      Format[DestinationConfig]
  ): Format[OutboxEvent] =
    Json.format[OutboxEvent]
