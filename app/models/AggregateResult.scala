package models

import play.api.libs.json.*

import java.time.Instant

case class AggregateResult(
    id: Long = 0L,
    aggregateId: String, // Business entity ID (e.g., order ID "123")
    eventType: String, // Last event type sent (e.g., "OrderCreated")
    destination: String, // Destination name (e.g., "inventory-webhook")
    endpointUrl: String, // Actual URL that was called (empty string for multi-route before selection)
    httpMethod: String, // HTTP method (POST, GET, etc.)
    requestPayload: Option[JsValue], // Request body sent
    responseStatus: Option[Int], // HTTP status code (200, 404, 500, etc.)
    responsePayload: Option[JsValue], // Response body received
    publishedAt: Instant = Instant.now(),
    durationMs: Option[Int], // Request duration in milliseconds
    success: Boolean, // Whether the request succeeded
    errorMessage: Option[String] = None // Error message if failed
)

object AggregateResult {
  given Format[AggregateResult] = Json.format[AggregateResult]
}
