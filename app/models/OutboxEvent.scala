package models

import play.api.libs.json.*

import java.time.Instant

/** Configuration for a single destination in an outbox event.
  *
  * Contains both the payload to send and path parameters for URL templating.
  * Path parameters are used to replace placeholders in URL templates.
  *
  * Example:
  * {{{
  *   DestinationConfig(
  *     payload = Some(Json.obj("amount" -> 99.99)),
  *     pathParams = Map("orderId" -> "123", "customerId" -> "customer-456")
  *   )
  *
  *   // Used with URL template: "/orders/{orderId}/customers/{customerId}"
  *   // Resolves to: "/orders/123/customers/customer-456"
  * }}}
  *
  * @param payload JSON payload to send to the destination (None for GET/DELETE without body)
  * @param pathParams URL path parameters for template substitution
  */
case class DestinationConfig(
    payload: Option[JsValue],
    pathParams: Map[String, String] = Map.empty
)

object DestinationConfig {
  given Format[DestinationConfig] = Json.format[DestinationConfig]
}

/** Tracks API responses from previous destinations for conditional routing.
  *
  * Enables decision chaining where one API's response influences the next API's route selection.
  * Essential for implementing conditional routing based on previous responses.
  *
  * Example:
  * {{{
  *   // Initial context (empty)
  *   val ctx = RoutingContext()
  *
  *   // After fraud check responds with {"riskScore": 75}
  *   val ctx2 = ctx.withResponse("fraudCheck", Json.obj("riskScore" -> 75))
  *
  *   // Billing can now route based on fraud check response:
  *   // if riskScore >= 50 => high-value-processing
  *   // if riskScore < 50  => standard billing
  * }}}
  *
  * @param destinationResponses Map of destination name to its JSON response
  */
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

/** Core outbox event that enables reliable, transactional event publishing.
  *
  * == Transactional Outbox Pattern ==
  *
  * This is the "to-do list" that gets saved atomically with business operations in the same
  * database transaction. Ensures events are never lost, even if:
  * - Application crashes after DB commit
  * - External APIs are temporarily down
  * - Network failures occur
  * - Message brokers (Kafka, etc.) are unavailable
  *
  * == Forward Events ==
  *
  * Regular events (e.g., "OrderCreated") that fan out to multiple destinations:
  * {{{
  *   OutboxEvent(
  *     aggregateId = "123",
  *     eventType   = "OrderCreated",
  *     payloads    = Map(
  *       "billing"   -> DestinationConfig(payload = Some(billingJson)),
  *       "shipping"  -> DestinationConfig(payload = Some(shippingJson)),
  *       "inventory" -> DestinationConfig(payload = Some(inventoryJson))
  *     )
  *   )
  * }}}
  *
  * == Revert Events (Compensation) ==
  *
  * Compensation can be triggered two ways:
  *
  * '''1. Automatic Compensation (on failure):'''
  * When a forward event exhausts retries, it moves to DLQ and automatically triggers compensation:
  * {{{
  *   // Forward event: inventory → fraudCheck → shipping → billing
  *   // If billing fails after 3 retries:
  *   // → Event moves to DLQ with status=PROCESSED, movedToDlq=true
  *   // → DLQProcessor automatically reverts: billing (skip, failed) → shipping → inventory
  * }}}
  *
  * '''2. Manual Compensation (user-initiated):'''
  * User cancellations use the `!` prefix to trigger manual compensation:
  * {{{
  *   OutboxEvent(
  *     aggregateId = "123",
  *     eventType   = "!OrderCreated",  // ! prefix triggers manual revert
  *     payloads    = Map.empty         // No payload needed, queries DB for forward results
  *   )
  * }}}
  *
  * Both compensation flows execute the same LIFO process:
  * 1. Query aggregate_results for successful forward calls
  * 2. Reverse the order (LIFO): '''[billing → shipping → inventory]'''
  * 3. Build revert endpoints from forward request/response data using JSONPath extraction
  * 4. Call revert APIs (e.g., POST /billing/{txId}/refund, POST /shipping/{shipmentId}/cancel)
  *
  * == Lifecycle ==
  *
  * 1. **PENDING** → Event ready to be processed (or waiting for nextRetryAt)
  * 2. **PROCESSING** → OutboxProcessor claimed the event
  * 3. **PROCESSED** → Successfully published to all destinations
  * 4. **PROCESSED (movedToDlq=true)** → Exhausted retries, moved to DLQ, triggers compensation
  *
  * Note: Failed events are marked as PROCESSED when moved to DLQ to prevent reprocessing.
  * The `movedToDlq` flag distinguishes successful completion from DLQ movement.
  *
  * == Retry Behavior ==
  *
  * - Exponential backoff: 2^1, 2^2, 2^3 seconds (configurable)
  * - Respects Retry-After headers (rate limiting)
  * - nextRetryAt tracks when to retry (NULL = ready now)
  *
  * == Idempotency ==
  *
  * The idempotencyKey prevents duplicate events:
  * - Format: `{aggregateId}:{eventType}`
  * - Example: `"123:OrderCreated"`
  * - Enforced by unique index: `CREATE UNIQUE INDEX ON outbox_events (idempotency_key) WHERE status != 'PROCESSED'`
  *
  * @param id Primary key (auto-generated)
  * @param aggregateId Business entity ID (e.g., order ID "123")
  * @param eventType Event name (e.g., "OrderCreated", "!OrderCreated" for revert)
  * @param payloads Destination-specific payloads with path parameters
  * @param createdAt When the event was created
  * @param status Current processing status
  * @param statusChangedAt When status last changed (for stale detection)
  * @param retryCount Number of retry attempts
  * @param lastError Last error message (if failed)
  * @param movedToDlq Whether event was moved to dead letter queue
  * @param idempotencyKey Unique key to prevent duplicates (format: aggregateId:eventType)
  * @param nextRetryAt When to retry (None = ready now, Some(instant) = retry at that time)
  */
case class OutboxEvent(
    id: Long = 0L,
    aggregateId: String,
    eventType: String,
    payloads: Map[String, DestinationConfig],
    createdAt: Instant               = Instant.now(),
    status: EventStatus              = EventStatus.Pending,
    statusChangedAt: Option[Instant] = None,
    retryCount: Int                  = 0,
    lastError: Option[String]        = None,
    movedToDlq: Boolean              = false,
    idempotencyKey: String           = "",
    nextRetryAt: Option[Instant]     = None
) extends RetryableEvent
    with AggregateEvent {

  /** Helper to generate idempotency key if not provided.
    * Idempotency key format: {aggregateId}:{eventType}
    */
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
