package publishers

import com.jayway.jsonpath.{ DocumentContext, JsonPath, Configuration as JsonPathConfig }
import play.api.Logger
import play.api.libs.json.*

import scala.util.Try

/** Builds revert (compensation) endpoints from forward API call results.
  *
  * == Purpose ==
  * Core utility for Saga compensation pattern. Extracts IDs and values from successful
  * forward API responses and uses them to construct revert API calls that undo the original operations.
  *
  * Used by:
  * - '''DLQProcessor''': Automatic compensation when forward events fail after max retries
  * - '''HttpEventPublisher''': Manual cancellation via !EventType events (e.g., user cancels order)
  *
  * == Example Scenario: Billing Refund ==
  *
  * '''Forward Call:'''
  * {{{
  * POST http://localhost:9000/api/billing
  * Request:  {"amount": 99.99, "currency": "USD"}
  * Response: {"transaction": {"id": "tx-456", "status": "completed"}, "chargedAmount": 99.99}
  * }}}
  *
  * '''Revert Config:'''
  * {{{
  * billing {
  *   url = "http://localhost:9000/api/billing"
  *   revert {
  *     url = "http://localhost:9000/api/billing/{transactionId}/refund"
  *     method = "POST"
  *     extract {
  *       transactionId = "response:$.transaction.id"  # Extract from forward response
  *       amount = "request:$.amount"                  # Extract from forward request
  *       currency = "request:$.currency"
  *     }
  *     payload = """{"transactionId": "{transactionId}", "amount": "{amount}", "currency": "{currency}", "reason": "order_failed"}"""
  *   }
  * }
  * }}}
  *
  * '''Built Revert Call:'''
  * {{{
  * POST http://localhost:9000/api/billing/tx-456/refund
  * Body: {"transactionId": "tx-456", "amount": "99.99", "currency": "USD", "reason": "order_failed"}
  * }}}
  *
  * == Real-World Examples ==
  *
  * '''1. Inventory Release (URL placeholders only):'''
  * {{{
  * inventory {
  *   url = "http://localhost:9000/api/inventory/reserve"
  *   revert {
  *     url = "http://localhost:9000/api/inventory/{reservationId}/release"
  *     method = "DELETE"
  *     extract {
  *       reservationId = "response:$.reservationId"
  *     }
  *   }
  * }
  *
  * # Forward response: {"reservationId": "RES-123", "quantity": 5}
  * # Built revert: DELETE http://localhost:9000/api/inventory/RES-123/release
  * }}}
  *
  * '''2. Shipping Cancellation (POST with payload):'''
  * {{{
  * shipping {
  *   routes = [{
  *     url = "http://localhost:9000/api/domestic-shipping"
  *     revert {
  *       url = "http://localhost:9000/api/domestic-shipping/{shipmentId}/cancel"
  *       method = "POST"
  *       extract {
  *         shipmentId = "response:$.shipmentId"
  *         orderId = "response:$.orderId"
  *         customerId = "request:$.customerId"
  *       }
  *       payload = """{"reason": "payment_failed", "shipmentId": "{shipmentId}", "orderId": "{orderId}", "customerId": "{customerId}"}"""
  *     }
  *   }]
  * }
  *
  * # Forward request:  {"customerId": "C-789", "shippingType": "domestic"}
  * # Forward response: {"shipmentId": "SHIP-DOM-123", "orderId": "456"}
  * # Built revert: POST http://localhost:9000/api/domestic-shipping/SHIP-DOM-123/cancel
  * # Body: {"reason": "payment_failed", "shipmentId": "SHIP-DOM-123", "orderId": "456", "customerId": "C-789"}
  * }}}
  *
  * '''3. High-Value Processing Cancellation (Multi-route):'''
  * {{{
  * billing {
  *   routes = [{
  *     url = "http://localhost:9000/api/billing"
  *     revert { /* standard refund */ }
  *   }, {
  *     url = "http://localhost:9000/api/high-value-processing"
  *     revert {
  *       url = "http://localhost:9000/api/high-value-processing/{processingId}/cancel"
  *       method = "POST"
  *       extract {
  *         processingId = "response:$.processingId"
  *         amount = "request:$.amount"
  *         currency = "request:$.currency"
  *       }
  *       payload = """{"processingId": "{processingId}", "amount": "{amount}", "currency": "{currency}", "reason": "order_failed"}"""
  *     }
  *   }]
  * }
  *
  * # If high-value route was used (riskScore >= 50):
  * # Forward URL: http://localhost:9000/api/high-value-processing
  * # Forward response: {"processingId": "HVP-789", "status": "approved"}
  * # Built revert: POST http://localhost:9000/api/high-value-processing/HVP-789/cancel
  * }}}
  *
  * == Extraction Prefix Notation ==
  *
  * - '''response:$.path''' - Extract from forward API response
  * - '''request:$.path''' - Extract from forward API request
  * - '''$.path''' - Defaults to response for backward compatibility
  *
  * == Nested JSONPath Extraction ==
  *
  * Supports deep JSON navigation:
  * {{{
  * extract {
  *   transactionId = "response:$.transaction.id"        # Nested: response.transaction.id
  *   customerId = "response:$.customer.profile.userId"  # Deeply nested
  *   amount = "request:$.payment.amount"                # Nested in request
  * }
  * }}}
  *
  * == Error Handling ==
  *
  * Fails with clear error messages if:
  * - JSONPath expression doesn't match (e.g., `$.transaction.id` but response is `{"txId": "123"}`)
  * - Required placeholder missing from URL template (e.g., `{orderId}` in URL but no `orderId` in extract)
  * - Source payload not available (e.g., extracting from request but requestPayload is None)
  *
  * == Multi-Route Destinations ==
  *
  * For destinations with multiple routes, each route has its own revert config.
  * The correct revert is matched by the forward URL that was actually called:
  * {{{
  * shipping {
  *   routes = [{
  *     url = "http://localhost:9000/api/domestic-shipping"
  *     revert {
  *       url = "http://localhost:9000/api/domestic-shipping/{shipmentId}/cancel"
  *       extract { shipmentId = "response:$.shipmentId" }
  *     }
  *   }, {
  *     url = "http://localhost:9000/api/international-shipping"
  *     revert {
  *       url = "http://localhost:9000/api/international-shipping/{shipmentId}/cancel"
  *       extract { shipmentId = "response:$.shipmentId" }
  *     }
  *   }]
  * }
  *
  * # If domestic route was used:
  * # findRevertConfig("shipping", "http://localhost:9000/api/domestic-shipping")
  * # → Returns domestic route's revert config
  * }}}
  */
object RevertEndpointBuilder {

  private val logger           = Logger(this.getClass)
  private val jsonPathConfig   = JsonPathConfig.defaultConfiguration()
  private val PlaceholderRegex = "\\{([A-Za-z0-9_.\\-]+)}".r

  extension (spec: String)
    /** Parses a payload spec into source (request/response) and JSONPath expression.
      * Supports:
      * - "request:$.path" → ("request", "$.path")
      * - "response:$.path" → ("response", "$.path")
      * - "$.path" → ("response", "$.path")
      */
    private def parseSourceAndPath: (String, String) = {
      val idx = spec.indexOf(':')
      if (idx > 0) {
        spec.substring(0, idx) match {
          case "request" => ("request", spec.substring(idx + 1))
          case "response" => ("response", spec.substring(idx + 1))
          case _ => ("response", spec)
        }
      } else ("response", spec)
    }

    /** Gets the JsonPath context for a given source (request or response).
      * Throws IllegalArgumentException if the requested source is not available.
      */
    private def contextFor(
        contexts: (Option[DocumentContext], Option[DocumentContext])
    ): DocumentContext = {
      val (requestCtxOpt, responseCtxOpt) = contexts
      spec match {
        case "request" =>
          requestCtxOpt.getOrElse(
            throw new IllegalArgumentException(
              "Cannot extract from request payload: request payload is not available"
            )
          )
        case "response" =>
          responseCtxOpt.getOrElse(
            throw new IllegalArgumentException(
              "Cannot extract from response payload: response payload is not available"
            )
          )
      }
    }

  /** Builds a revert endpoint configuration from a forward result.
    *
    * == Flow ==
    * {{{
    * 1. Extract placeholder values from request/response using JSONPath
    *    extract { transactionId = "response:$.transaction.id", amount = "request:$.amount" }
    *    → Map("transactionId" -> "tx-456", "amount" -> "99.99")
    *
    * 2. Build revert URL by replacing placeholders with extracted values
    *    Template: "http://localhost:9000/api/billing/{transactionId}/refund"
    *    → "http://localhost:9000/api/billing/tx-456/refund"
    *
    * 3. Build revert payload (if template provided) by replacing placeholders
    *    Template: """{"transactionId": "{transactionId}", "amount": "{amount}", "reason": "order_failed"}"""
    *    → {"transactionId": "tx-456", "amount": "99.99", "reason": "order_failed"}
    *
    * 4. Return HttpEndpointConfig for the revert call
    *    HttpEndpointConfig(
    *      destinationName = "billing.revert",
    *      url = Some("http://localhost:9000/api/billing/tx-456/refund"),
    *      method = "POST",
    *      ...
    *    )
    * }}}
    *
    * == Example: Billing Refund ==
    * {{{
    * # Inputs
    * revertConfig = RevertConfig(
    *   url = "http://localhost:9000/api/billing/{transactionId}/refund",
    *   method = "POST",
    *   extract = Map(
    *     "transactionId" -> "response:$.transaction.id",
    *     "amount" -> "request:$.amount",
    *     "currency" -> "request:$.currency"
    *   ),
    *   payload = Some("""{"transactionId": "{transactionId}", "amount": "{amount}", "currency": "{currency}", "reason": "order_failed"}""")
    * )
    * requestPayload = Some({"amount": 99.99, "currency": "USD"})
    * responsePayload = Some({"transaction": {"id": "tx-456", "status": "completed"}})
    * baseDestination = "billing.revert"
    *
    * # Output
    * Success(
    *   (
    *     HttpEndpointConfig(
    *       destinationName = "billing.revert",
    *       url = Some("http://localhost:9000/api/billing/tx-456/refund"),
    *       method = "POST",
    *       ...
    *     ),
    *     Some({"transactionId": "tx-456", "amount": "99.99", "currency": "USD", "reason": "order_failed"})
    *   )
    * )
    * }}}
    *
    * @param revertConfig The revert configuration from the destination
    * @param requestPayload The original forward request payload
    * @param responsePayload The original forward response payload
    * @param baseDestination The base destination name (e.g., "billing.revert")
    * @param headers Additional headers to include in the revert HTTP call
    * @param timeout HTTP timeout for the revert call
    * @return Success with (HttpEndpointConfig, Optional payload), or Failure if extraction fails
    */
  def buildRevertEndpoint(
      revertConfig: RevertConfig,
      requestPayload: Option[JsValue],
      responsePayload: Option[JsValue],
      baseDestination: String,
      headers: Map[String, String],
      timeout: scala.concurrent.duration.FiniteDuration
  ): Try[(HttpEndpointConfig, Option[JsValue])] =
    for {
      placeholderValues <- extractPlaceholderValues(revertConfig, requestPayload, responsePayload)
      revertUrl <- buildRevertUrl(revertConfig.url, placeholderValues)
    } yield (
      HttpEndpointConfig(
        destinationName = baseDestination,
        url             = Some(revertUrl),
        method          = revertConfig.method,
        headers         = headers,
        timeout         = timeout,
        revert          = None // Reverts don't have nested reverts
      ),
      buildRevertPayload(revertConfig, placeholderValues)
    )

  /** Builds revert payload from the template. If no template is provided, returns None. */
  private def buildRevertPayload(
      revertConfig: RevertConfig,
      placeholderValues: Map[String, String]
  ): Option[JsValue] = revertConfig.payload.flatMap { template =>
    Try(Json.parse(placeholderValues.replacePlaceholders(template))).toOption
  }

  /** Extracts placeholder values from request and/or response payloads using JSONPath.
    * Supports prefix notation:
    * - "request:$.amount" - extract from request payload
    * - "response:$.id"    - extract from response payload
    * - "$.id"             - defaults to response payload for backward compatibility
    *
    * This implementation avoids repeated JSON parsing by pre-parsing each payload once
    * and uses a Map builder to minimize intermediate allocations.
    */
  private def extractPlaceholderValues(
      revertConfig: RevertConfig,
      requestPayload: Option[JsValue],
      responsePayload: Option[JsValue]
  ): Try[Map[String, String]] = Try {

    revertConfig.extract.map { case (placeholderName, pathSpec) =>
      val (source, jsonPathExpr) = pathSpec.parseSourceAndPath
      val ctx = source.contextFor(
        (
          requestPayload.map(_.toString).map(s => JsonPath.using(jsonPathConfig).parse(s)),
          responsePayload.map(_.toString).map(s => JsonPath.using(jsonPathConfig).parse(s))
        )
      )

      val valueAny = Try(ctx.read[Any](jsonPathExpr)).getOrElse {
        throw new IllegalArgumentException(
          s"JSONPath expression '$jsonPathExpr' did not match any value in $source payload"
        )
      }

      placeholderName -> valueAny.toString
    }
  }

  /** Builds the revert URL by replacing placeholders with extracted values.
    * - Fails with error if any placeholders are missing
    */
  private def buildRevertUrl(
      urlTemplate: String,
      placeholderValues: Map[String, String]
  ): Try[String] = Try {
    val placeholdersInTemplate = PlaceholderRegex.findAllMatchIn(urlTemplate).map(_.group(1)).toSet

    if (placeholdersInTemplate.isEmpty) urlTemplate
    else {
      val missing = placeholdersInTemplate.diff(placeholderValues.keySet)
      if (missing.nonEmpty)
        throw new IllegalArgumentException(
          s"Missing placeholders: ${missing.toList.sorted.mkString(", ")} in URL template: $urlTemplate"
        )

      val unused = placeholderValues.keySet.diff(placeholdersInTemplate)
      if (unused.nonEmpty)
        logger.debug(
          s"Ignoring unused revert placeholders: ${unused.toList.sorted.mkString(", ")} in URL template: $urlTemplate"
        )

      placeholderValues.replacePlaceholders(urlTemplate)
    }
  }
}
