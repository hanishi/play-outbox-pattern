package publishers

import com.jayway.jsonpath
import models.{ OutboxEvent, RoutingContext }
import play.api.Logger
import play.api.libs.json.*

import scala.concurrent.duration.*
import scala.util.Try

/** Comparison operators for conditional routing.
  *
  * Supports both string and numeric comparisons for routing decisions.
  *
  * == Operators ==
  * - '''Eq/Ne''': String equality/inequality
  * - '''Gt/Gte/Lt/Lte''': Numeric comparisons (converts to Double)
  * - '''Contains''': String contains substring
  * - '''Exists''': Field exists in JSON (ignores value)
  *
  * == Examples ==
  * {{{
  * // String comparison
  * Condition("$.shippingType", Eq, Some("domestic"))
  *
  * // Numeric comparison
  * Condition("$.riskScore", Gte, Some("50"), previousDestination = Some("fraudCheck"))
  *
  * // Existence check
  * Condition("$.premium", Exists, None)
  * }}}
  */
enum ComparisonOperator:
  case Eq, Ne, Gt, Gte, Lt, Lte, Contains, Exists

object ComparisonOperator:
  given Reads[ComparisonOperator] = Reads {
    case JsString(s) => fromString(s).map(JsSuccess(_)).getOrElse(JsError(s"Invalid operator: $s"))
    case _ => JsError("Expected string for ComparisonOperator")
  }

  def fromString(s: String): Option[ComparisonOperator] = s.toLowerCase match
    case "eq" => Some(Eq)
    case "ne" => Some(Ne)
    case "gt" => Some(Gt)
    case "gte" => Some(Gte)
    case "lt" => Some(Lt)
    case "lte" => Some(Lte)
    case "contains" => Some(Contains)
    case "exists" => Some(Exists)
    case _ => None

  given Writes[ComparisonOperator] = Writes { op =>
    JsString(op.toString.toLowerCase)
  }

/** Format for Retry-After header values returned by external APIs.
  *
  * Many third-party APIs implement rate limiting and return Retry-After headers in various formats.
  *
  * == Formats ==
  * - '''Auto''': Auto-detect format (tries all formats until one succeeds)
  * - '''Seconds''': Integer seconds (e.g., "120")
  * - '''Milliseconds''': Integer milliseconds (e.g., "120000")
  * - '''Unix''': Unix timestamp (e.g., "1704623400")
  * - '''HttpDate''': HTTP date format (e.g., "Sun, 07 Jan 2025 10:30:00 GMT")
  * - '''Iso8601''': ISO 8601 format (e.g., "2025-01-07T10:30:00Z")
  *
  * == Example ==
  * {{{
  * retryAfter {
  *   headerName = "Retry-After"
  *   format = "auto"  # Auto-detect format
  *   fallbackSeconds = 30  # Default if header missing
  * }
  * }}}
  */
enum RetryAfterFormat:
  case Auto, Seconds, Milliseconds, Unix, HttpDate, Iso8601

object RetryAfterFormat:
  given Reads[RetryAfterFormat] = Reads {
    case JsString(s) => fromString(s).map(JsSuccess(_)).getOrElse(JsError(s"Invalid format: $s"))
    case _ => JsError("Expected string for RetryAfterFormat")
  }

  def fromString(s: String): Option[RetryAfterFormat] = s.toLowerCase match
    case "auto" => Some(Auto)
    case "seconds" => Some(Seconds)
    case "milliseconds" => Some(Milliseconds)
    case "unix" => Some(Unix)
    case "httpdate" => Some(HttpDate)
    case "iso8601" => Some(Iso8601)
    case _ => None

  given Writes[RetryAfterFormat] = Writes { format =>
    JsString(format.toString.toLowerCase)
  }

/** Configuration for parsing Retry-After headers from API responses.
  * @param headerName Name of the header to read (default: "Retry-After")
  * @param format Format of the value
  * @param fromBody If true, read from response body instead of header
  * @param jsonPath JSONPath to extract value from response body (requires fromBody=true)
  * @param fallbackSeconds Fallback delay in seconds if parsing fails
  */
case class RetryAfterConfig(
    headerName: String            = "Retry-After",
    format: RetryAfterFormat      = RetryAfterFormat.Auto,
    fromBody: Boolean             = false,
    jsonPath: Option[String]      = None,
    fallbackSeconds: Option[Long] = None
)

/** Configuration for reverting a successful operation.
  * @param url URL template with placeholders (e.g., "http://api.com/revert/{id}")
  * @param method HTTP method for revert call (typically DELETE or POST)
  * @param extract Map of placeholder names to JSONPath expressions for extracting values
  *                Supports extracting from both request and response:
  *                - "response:$.data.id" - extract from response payload
  *                - "request:$.amount" - extract from request payload
  *                - "$.id" - defaults to response payload for backward compatibility
  *                Example: Map("id" -> "response:$.transaction.id", "amount" -> "request:$.amount")
  * @param payload Optional JSON payload template with placeholders (e.g., """{"reason": "order_failed", "inventoryId": "{id}"}""")
  *                Placeholders will be replaced with values extracted via extract
  */
case class RevertConfig(
    url: String,
    method: String,
    extract: Map[String, String],
    payload: Option[String]
)

/** Condition for conditional routing based on previous step results.
  * @param jsonPath JSONPath expression to extract value from the previous response
  * @param operator Comparison operator
  * @param value Expected value to compare against (optional for "exists")
  * @param previousDestination Which previous destination's response to check (None = immediate previous)
  *
  * Examples:
  * - Condition("$.status", ComparisonOperator.Eq, Some("approved")) - Execute if previous response status == "approved"
  * - Condition("$.amount", ComparisonOperator.Gt, Some("1000")) - Execute if amount > 1000
  * - Condition("$.premium", ComparisonOperator.Eq, Some("true")) - Execute if premium == true
  * - Condition("$.riskScore", ComparisonOperator.Exists, None) - Execute if riskScore field exists
  */
case class Condition(
    jsonPath: String,
    operator: ComparisonOperator,
    value: Option[String]               = None,
    previousDestination: Option[String] = None // If None, uses immediate previous destination
)

/** Evaluates conditions for conditional routing based on previous API responses or request payload.
  *
  * == Purpose ==
  * Enables dynamic routing decisions based on data from previous destinations in a fan-out chain.
  * Essential for implementing workflows where one API's response influences the next API's route.
  *
  * == Use Cases ==
  *
  * '''1. Route based on previous destination response:'''
  * {{{
  * # Config: Billing routes based on fraud check risk score
  * billing {
  *   method = "POST"
  *   routes = [{
  *     # Low-risk orders: use standard billing
  *     url = "http://localhost:9000/api/billing"
  *     condition {
  *       jsonPath = "$.riskScore"
  *       operator = "lt"
  *       value = "50"
  *       previousDestination = "fraudCheck"  # Check fraudCheck API response
  *     }
  *   }, {
  *     # High-risk orders: use high-value processor
  *     url = "http://localhost:9000/api/high-value-processing"
  *     condition {
  *       jsonPath = "$.riskScore"
  *       operator = "gte"
  *       value = "50"
  *       previousDestination = "fraudCheck"
  *     }
  *   }]
  * }
  *
  * # Flow
  * 1. fraudCheck API called → returns {"riskScore": 75, "reason": "high_value_customer"}
  * 2. Billing evaluates conditions against fraudCheck's response
  * 3. Second condition matches (riskScore >= 50)
  * 4. Routes to http://localhost:9000/api/high-value-processing
  * }}}
  *
  * '''2. Route based on request payload:'''
  * {{{
  * # Config: Shipping routes based on shipping type in request
  * shipping {
  *   method = "POST"
  *   routes = [{
  *     url = "http://localhost:9000/api/domestic-shipping"
  *     condition {
  *       jsonPath = "$.shippingType"
  *       operator = "eq"
  *       value = "domestic"
  *       # No previousDestination = check request payload
  *     }
  *   }, {
  *     url = "http://localhost:9000/api/international-shipping"
  *     condition {
  *       jsonPath = "$.shippingType"
  *       operator = "eq"
  *       value = "international"
  *     }
  *   }]
  * }
  *
  * # Flow
  * 1. Request payload: {"customerId": "user-123", "shippingType": "domestic", "totalAmount": 99.99}
  * 2. Shipping evaluates conditions against request payload
  * 3. First condition matches (shippingType == "domestic")
  * 4. Routes to http://localhost:9000/api/domestic-shipping
  * }}}
  *
  * '''3. Status-based workflow routing:'''
  * {{{
  * # Config: Notifications route based on order status
  * notifications {
  *   method = "POST"
  *   routes = [{
  *     # Send shipping confirmation when SHIPPED
  *     url = "http://localhost:9000/api/notifications/shipping-confirmation"
  *     condition { jsonPath = "$.newStatus", operator = "eq", value = "SHIPPED" }
  *   }, {
  *     # Send review request when DELIVERED
  *     url = "http://localhost:9000/api/notifications/review-request"
  *     condition { jsonPath = "$.newStatus", operator = "eq", value = "DELIVERED" }
  *   }, {
  *     # Catch-all for other status changes
  *     url = "http://localhost:9000/api/notifications/status-update"
  *     condition { jsonPath = "$.newStatus", operator = "ne", value = "" }
  *   }]
  * }
  *
  * # Flow
  * 1. OrderStatusUpdated event: {"orderId": 123, "newStatus": "SHIPPED", "oldStatus": "PENDING"}
  * 2. Notifications evaluates conditions against request payload
  * 3. First condition matches (newStatus == "SHIPPED")
  * 4. Routes to shipping-confirmation endpoint
  * }}}
  *
  * == Supported Operators ==
  *
  * - '''exists''': Field exists in JSON (value ignored)
  * - '''eq''': String equality
  * - '''ne''': String inequality
  * - '''contains''': String contains substring
  * - '''gt/gte/lt/lte''': Numeric comparisons (converts to Double)
  *
  * == JSONPath Evaluation ==
  *
  * Uses jayway-jsonpath library for JSON extraction:
  * - `$.riskScore` - Extract top-level field
  * - `$.transaction.id` - Extract nested field
  * - `$.items[0].sku` - Extract from array
  *
  * If JSONPath doesn't match or a value can't be parsed, the condition evaluates to false.
  *
  * == Routing Context ==
  *
  * RoutingContext tracks all previous destination responses in the current fan-out chain.
  * Example fan-out: inventory → fraudCheck → shipping → billing
  * - fraudCheck sees: {inventory: response1}
  * - shipping sees: {inventory: response1, fraudCheck: response2}
  * - billing sees: {inventory: response1, fraudCheck: response2, shipping: response3}
  */
object Condition:
  val logger              = Logger(this.getClass)
  given Format[Condition] = Json.format[Condition]

  extension (condition: Condition) {

    /** Evaluates a condition against the routing context or request payload.
      *
      * Logic:
      * 1. If previousDestination specified → extract that destination's response from context
      * 2. If no previousDestination → use request payload
      * 3. Apply JSONPath to extract value
      * 4. Compare with operator
      *
      * @param routingContext The routing context with previous responses
      * @param requestPayload The original request payload (used when no previousDestination specified)
      * @return true if the condition is met, false otherwise
      */
    def evaluate(
        routingContext: RoutingContext,
        requestPayload: JsValue
    ): Boolean = condition.previousDestination
      .map { dest =>
        val responseOpt = routingContext.getResponse(dest)
        if (responseOpt.isEmpty) {
          logger.warn(s"No response found for destination: $dest")
        }
        responseOpt
      }
      .getOrElse(Some(requestPayload))
      .exists(json => evaluateCondition(json))

    private def evaluateCondition(json: JsValue): Boolean =
      Try(jsonpath.JsonPath.read[Any](json.toString, condition.jsonPath)).toOption
        .map(_.toString)
        .exists { extractedStr =>
          condition.operator match {
            case ComparisonOperator.Exists => true

            case ComparisonOperator.Eq =>
              condition.value.contains(extractedStr)

            case ComparisonOperator.Ne =>
              condition.value.exists(_ != extractedStr)

            case ComparisonOperator.Contains =>
              condition.value.exists(extractedStr.contains)

            case ComparisonOperator.Gt =>
              compareNumeric(extractedStr, condition.value)(_ > _)

            case ComparisonOperator.Gte =>
              compareNumeric(extractedStr, condition.value)(_ >= _)

            case ComparisonOperator.Lt =>
              compareNumeric(extractedStr, condition.value)(_ < _)

            case ComparisonOperator.Lte =>
              compareNumeric(extractedStr, condition.value)(_ <= _)
          }
        }

    private inline def compareNumeric(
        extractedStr: String,
        valueOpt: Option[String]
    )(cmp: (Double, Double) => Boolean): Boolean = (for {
      expectedStr <- valueOpt
      extractedNum <- extractedStr.toDoubleOption
      expectedNum <- expectedStr.toDoubleOption
    } yield cmp(extractedNum, expectedNum)).getOrElse(false)
  }

/** Represents a single route option within a multi-route destination.
  *
  * Enables a single logical destination to route to different physical endpoints
  * based on runtime conditions (request payload or previous API responses).
  *
  * == Purpose ==
  * Allows conditional routing where one destination name maps to multiple URLs,
  * with the first matching condition determining which URL to call.
  *
  * == Example ==
  * {{{
  * # Billing destination with two routes based on risk score
  * billing {
  *   routes = [{
  *     url = "http://localhost:9000/api/billing"
  *     condition { jsonPath = "$.riskScore", operator = "lt", value = "50", previousDestination = "fraudCheck" }
  *     revert {
  *       url = "http://localhost:9000/api/billing/{transactionId}/refund"
  *       method = "POST"
  *       extract { transactionId = "response:$.transaction.id" }
  *     }
  *   }, {
  *     url = "http://localhost:9000/api/high-value-processing"
  *     condition { jsonPath = "$.riskScore", operator = "gte", value = "50", previousDestination = "fraudCheck" }
  *     revert {
  *       url = "http://localhost:9000/api/high-value-processing/{processingId}/cancel"
  *       method = "POST"
  *       extract { processingId = "response:$.processingId" }
  *     }
  *   }]
  * }
  * }}}
  *
  * @param url The endpoint URL to call if this route's condition matches
  * @param condition The condition that must be satisfied for this route to be selected
  * @param revert Optional revert configuration specific to this route (for Saga compensation)
  */
case class RouteOption(
    url: String,
    condition: Condition,
    revert: Option[RevertConfig] = None
)

/** HTTP endpoint configuration for external API calls.
  *
  * Represents a destination that can be either single-route (with a single URL)
  * or multi-route (with multiple URL options selected via conditions).
  *
  * == Single-Route Destination ==
  * {{{
  * inventory {
  *   url = "http://localhost:9000/api/inventory/reserve"
  *   method = "POST"
  *   timeout = 5 seconds
  *   revert {
  *     url = "http://localhost:9000/api/inventory/{reservationId}/release"
  *     method = "DELETE"
  *     extract { reservationId = "response:$.reservationId" }
  *   }
  * }
  * }}}
  *
  * == Multi-Route Destination ==
  * {{{
  * shipping {
  *   method = "POST"
  *   timeout = 5 seconds
  *   routes = [{
  *     url = "http://localhost:9000/api/domestic-shipping"
  *     condition { jsonPath = "$.shippingType", operator = "eq", value = "domestic" }
  *     revert {
  *       url = "http://localhost:9000/api/domestic-shipping/{shipmentId}/cancel"
  *       method = "POST"
  *       extract { shipmentId = "response:$.shipmentId" }
  *     }
  *   }, {
  *     url = "http://localhost:9000/api/international-shipping"
  *     condition { jsonPath = "$.shippingType", operator = "eq", value = "international" }
  *     revert {
  *       url = "http://localhost:9000/api/international-shipping/{shipmentId}/cancel"
  *       method = "POST"
  *       extract { shipmentId = "response:$.shipmentId" }
  *     }
  *   }]
  * }
  * }}}
  *
  * == Rate Limiting Support ==
  * {{{
  * fraudCheck {
  *   url = "http://localhost:9000/api/fraud/check"
  *   method = "POST"
  *   retryAfter {
  *     headerName = "Retry-After"
  *     format = "auto"  # Auto-detect: seconds, unix, ISO 8601, etc.
  *     fallbackSeconds = 30  # Default if header missing
  *   }
  * }
  * }}}
  *
  * @param url Single URL (None for multi-route destinations)
  * @param method HTTP method (GET, POST, PUT, DELETE, etc.)
  * @param headers Custom HTTP headers to include in the request
  * @param timeout Request timeout (default: 30 seconds)
  * @param destinationName Logical name of this destination (for payload lookup)
  * @param revert Revert configuration for Saga compensation (single-route only)
  * @param retryAfter Configuration for parsing rate limit headers
  * @param condition Condition for single-route conditional execution (deprecated, use routes instead)
  * @param routes Multiple route options with conditions (for multi-route destinations)
  */
case class HttpEndpointConfig(
    url: Option[String],
    method: String               = "POST",
    headers: Map[String, String] = Map.empty,
    timeout: FiniteDuration      = 30.seconds,
    destinationName: String      = "",
    revert: Option[RevertConfig] = None,
    retryAfter: RetryAfterConfig = RetryAfterConfig(),
    condition: Option[Condition] = None,
    routes: List[RouteOption]    = List.empty
) {
  inline def withResolvedUrl(resolved: String): HttpEndpointConfig =
    copy(url = Some(resolved))
}

/** Routed destination types.
  * Destinations can be either forward calls or revert (compensation) calls.
  */
enum RoutedDestination:
  /** A forward destination that makes a normal HTTP call to an API.
    * This is the default type for all configured destinations.
    */
  case Forward(endpoint: HttpEndpointConfig)

  /** A virtual destination that performs a revert/compensation operation.
    * These are auto-generated for revert events (prefixed with !).
    * Example: !OrderCreated generates Revert destinations for "billing", "shipping", etc.
    *
    * @param baseDestination The name of the forward destination (e.g., "billing")
    *                        All config (timeout, headers, revert) will be looked up from router at runtime
    */
  case Revert(baseDestination: String)

/** Core abstraction for routing outbox events to external API destinations.
  *
  * == Purpose ==
  * Defines how events are routed to HTTP endpoints, supporting:
  * - '''Fan-out''': Single event → multiple destinations
  * - '''Conditional routing''': Dynamic URL selection based on payload or previous responses
  * - '''Saga compensation''': Automatic revert routing with LIFO ordering
  *
  * == Implementations ==
  * - '''ConfigurationEventRouter''': Routes based on application.conf (default)
  * - Custom routers can implement this trait for dynamic routing logic
  *
  * == Forward Routing ==
  * {{{
  * # Config: OrderCreated fans out to 4 destinations
  * fanout {
  *   OrderCreated = ["inventory", "fraudCheck", "shipping", "billing"]
  * }
  *
  * # Code
  * val event = OutboxEvent(eventType = "OrderCreated", ...)
  * val destinations = router.routeEventToDestinations(event)
  * // Returns: [Forward(inventory), Forward(fraudCheck), Forward(shipping), Forward(billing)]
  * }}}
  *
  * == Revert Routing (Saga Compensation) ==
  * {{{
  * # Automatic revert triggered when forward event moves to DLQ
  * # OR manual revert via !OrderCreated event
  *
  * val revertEvent = OutboxEvent(eventType = "!OrderCreated", ...)
  * val destinations = router.routeEventToDestinations(revertEvent)
  * // Returns LIFO order: [Revert("billing"), Revert("shipping"), Revert("inventory")]
  * // fraudCheck omitted (read-only, no revert config)
  * }}}
  *
  * == Multi-Route Conditional Routing ==
  * {{{
  * # Billing routes to different URLs based on fraudCheck response
  * billing {
  *   routes = [{
  *     url = "http://localhost:9000/api/billing"
  *     condition { jsonPath = "$.riskScore", operator = "lt", value = "50", previousDestination = "fraudCheck" }
  *   }, {
  *     url = "http://localhost:9000/api/high-value-processing"
  *     condition { jsonPath = "$.riskScore", operator = "gte", value = "50", previousDestination = "fraudCheck" }
  *   }]
  * }
  *
  * # At runtime:
  * # 1. fraudCheck responds with {"riskScore": 75}
  * # 2. Billing evaluates conditions
  * # 3. Second route matches (75 >= 50)
  * # 4. Calls high-value-processing endpoint
  * }}}
  *
  * == Key Methods ==
  *
  * - '''routeEventToDestinations''': Fan-out routing, returns Forward or Revert destinations
  * - '''routeEvent''': Single-destination routing (legacy, use routeEventToDestinations instead)
  * - '''findRevertConfig''': Locates revert config for a given forward URL
  */
trait EventRouter {
  /** Legacy single-destination routing method.
    *
    * @deprecated Use routeEventToDestinations for fan-out support
    * @param event The outbox event to route
    * @return HTTP endpoint configuration for the event type
    * @throws IllegalStateException if no route is configured for the event type
    */
  def routeEvent(event: OutboxEvent): HttpEndpointConfig

  /** Routes an event to multiple destinations (fan-out).
    *
    * == Forward Events ==
    * Returns Forward destinations in configured order:
    * {{{
    * OrderCreated → [Forward(inventory), Forward(fraudCheck), Forward(shipping), Forward(billing)]
    * }}}
    *
    * == Revert Events ==
    * Returns Revert destinations in reversed LIFO order:
    * {{{
    * !OrderCreated → [Revert("billing"), Revert("shipping"), Revert("inventory")]
    * }}}
    *
    * The runtime (HttpEventPublisher) handles:
    * - Forward: Conditional routing, payload validation, API calls
    * - Revert: Query aggregate_results, build revert endpoints, call revert APIs, skip if no revert config
    *
    * @param event The outbox event to route
    * @return List of destinations (Forward or Revert), empty if no routes configured
    */
  def routeEventToDestinations(event: OutboxEvent): List[RoutedDestination]

  /** Finds the correct revert config for a destination based on the forward URL.
    *
    * == Single-Route Destinations ==
    * Returns top-level revert config:
    * {{{
    * inventory {
    *   url = "http://localhost:9000/api/inventory/reserve"
    *   revert { url = "http://localhost:9000/api/inventory/{reservationId}/release" }
    * }
    * // findRevertConfig("inventory", "...") → Some(RevertConfig(...))
    * }}}
    *
    * == Multi-Route Destinations ==
    * Matches forward URL to find the correct route's revert config:
    * {{{
    * billing {
    *   routes = [{
    *     url = "http://localhost:9000/api/billing"
    *     revert { url = "http://localhost:9000/api/billing/{txId}/refund" }
    *   }, {
    *     url = "http://localhost:9000/api/high-value-processing"
    *     revert { url = "http://localhost:9000/api/high-value-processing/{id}/cancel" }
    *   }]
    * }
    * // findRevertConfig("billing", "http://localhost:9000/api/billing") → Some(RevertConfig(refund))
    * // findRevertConfig("billing", "http://localhost:9000/api/high-value-processing") → Some(RevertConfig(cancel))
    * }}}
    *
    * @param baseDestination The base destination name (e.g., "billing")
    * @param forwardUrl The URL that was called in the forward operation
    * @return The correct revert config, or None if not found or not configured
    */
  def findRevertConfig(baseDestination: String, forwardUrl: String): Option[RevertConfig]
}

/** Extension methods for placeholder replacement in templates */
extension (placeholders: Map[String, String])
  /** Replaces all {key} placeholders in the template with corresponding values from the map.
    * Used for URL templates and JSON payload templates.
    */
  def replacePlaceholders(template: String): String =
    placeholders.foldLeft(template) { case (acc, (key, value)) =>
      acc.replace(s"{$key}", value)
    }
