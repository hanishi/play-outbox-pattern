package models

import play.api.libs.json.*

import java.time.Instant

/** Audit log of every HTTP call made to external APIs.
  *
  * == Purpose ==
  *
  * The aggregate_results table serves three critical functions:
  *
  * 1. '''Compensation (Saga Pattern)''': Tracks successful forward calls so they can be undone
  * 2. '''Audit Trail''': Complete history of all API interactions for debugging and compliance
  * 3. '''Idempotency''': Prevents duplicate calls to the same destination for the same event
  *
  * == Forward vs Revert Results ==
  *
  * Results use the eventType field to distinguish between forward and revert flows:
  *
  * '''Forward Result:'''
  * {{{
  *   AggregateResult(
  *     aggregateId = "123",
  *     eventType = "OrderCreated",                      // No suffix
  *     destination = "billing",
  *     endpointUrl = "http://localhost:9000/api/billing",
  *     requestPayload = Some(Json.obj("amount" -> 99.99)),
  *     responsePayload = Some(Json.obj("transactionId" -> "TXN-456")),
  *     success = true
  *   )
  * }}}
  *
  * '''Revert Result (Compensation):'''
  * {{{
  *   AggregateResult(
  *     aggregateId = "123",
  *     eventType = "OrderCreated:REVERT",               // :REVERT suffix
  *     destination = "billing",
  *     endpointUrl = "http://localhost:9000/api/billing/TXN-456/refund",
  *     requestPayload = Some(Json.obj("transactionId" -> "TXN-456", "amount" -> 99.99)),
  *     responsePayload = Some(Json.obj("refundId" -> "REF-789")),
  *     success = true
  *   )
  * }}}
  *
  * == Unique Constraint ==
  *
  * Results are upserted based on (aggregate_id, destination, event_type):
  * - Ensures only one result per aggregate+destination+event
  * - Allows both forward and revert results to coexist (different event_type)
  * - Prevents duplicate processing
  *
  * == Response Data for Compensation ==
  *
  * The responsePayload contains IDs needed for revert operations:
  * {{{
  *   // Forward call to shipping API:
  *   responsePayload = {"shipmentId": "SHIP-789", "trackingNumber": "..."}
  *
  *   // Later, during compensation:
  *   // Extract shipmentId from response using JSONPath: $.shipmentId
  *   // Build revert URL: /api/shipping/SHIP-789/cancel
  * }}}
  *
  * == LIFO Ordering (Saga Pattern) ==
  *
  * The fanoutOrder and publishedAt fields ensure proper LIFO compensation:
  * {{{
  *   Forward: billing (order=0) → shipping (order=1) → inventory (order=2)
  *   Revert:  inventory (order=2) → shipping (order=1) → billing (order=0)
  * }}}
  *
  * DLQProcessor queries in reverse order to ensure dependencies are respected.
  *
  * @param id Primary key (auto-generated)
  * @param aggregateId Business entity ID (e.g., order ID "123")
  * @param eventType Event type (e.g., "OrderCreated" or "OrderCreated:REVERT")
  * @param destination Destination name (e.g., "billing", "shipping")
  * @param endpointUrl Actual URL that was called (after route selection and path param substitution)
  * @param httpMethod HTTP method used (POST, GET, DELETE, etc.)
  * @param requestPayload JSON request body sent
  * @param responseStatus HTTP status code received (200, 404, 500, etc.)
  * @param responsePayload JSON response body received (contains IDs for compensation)
  * @param publishedAt Timestamp when the call was made (used for LIFO ordering)
  * @param durationMs Request duration in milliseconds
  * @param success Whether the request succeeded (200-299 status or DELETE+404)
  * @param errorMessage Error message if failed
  * @param fanoutOrder Position in fanout configuration (0-indexed, used for LIFO ordering)
  */
case class AggregateResult(
    id: Long = 0L,
    aggregateId: String,
    eventType: String,
    destination: String,
    endpointUrl: String,
    httpMethod: String,
    requestPayload: Option[JsValue],
    responseStatus: Option[Int],
    responsePayload: Option[JsValue],
    publishedAt: Instant = Instant.now(),
    durationMs: Option[Int],
    success: Boolean,
    errorMessage: Option[String] = None,
    fanoutOrder: Int = 0
)

object AggregateResult {
  given Format[AggregateResult] = Json.format[AggregateResult]
}
