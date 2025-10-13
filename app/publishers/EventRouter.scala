package publishers

import models.OutboxEvent
import play.api.libs.json.*

import scala.concurrent.duration.*

/** Comparison operators for conditional routing */
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

/** Format for Retry-After values */
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

object Condition:
  given Format[Condition] = Json.format[Condition]

/** Represents a single route option within a multi-route destination.
  * Used when a destination can route to different URLs based on conditions.
  */
case class RouteOption(
    url: String,
    condition: Condition,
    revert: Option[RevertConfig] = None
)

case class HttpEndpointConfig(
    url: Option[String], // empty if routes are configured
    method: String               = "POST",
    headers: Map[String, String] = Map.empty,
    timeout: FiniteDuration      = 30.seconds,
    destinationName: String      = "", // Name of the destination (for payload lookup)
    revert: Option[RevertConfig] = None, // Optional revert configuration
    retryAfter: RetryAfterConfig = RetryAfterConfig(), // Retry-After header parsing config
    condition: Option[Condition] = None, // Optional condition for conditional execution
    routes: List[RouteOption]    = List.empty // Optional multiple routes with conditions
) {
  inline def withResolvedUrl(resolved: String): HttpEndpointConfig =
    copy(url = Some(resolved))
}

trait EventRouter {
  def routeEvent(event: OutboxEvent): HttpEndpointConfig

  /** Routes an event to multiple destinations for fan-out publishing.
    * If no fan-out is configured, returns a single-element list with the default destination.
    * Destinations are returned in order and will be published sequentially.
    */
  def routeEventToMultiple(event: OutboxEvent): List[HttpEndpointConfig]
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
