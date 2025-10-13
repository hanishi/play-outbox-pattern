package controllers

import play.api.libs.json.*
import play.api.mvc.*
import play.api.{ Configuration, Logging }

import javax.inject.*
import scala.util.Random

/** Simulates external webhook endpoints for testing the outbox pattern.
  *
  * Features:
  * - Random failures (configurable rate)
  * - Slow responses (configurable delay)
  * - Request logging
  * - Different response codes
  */
@Singleton
class WebhookSimulatorController @Inject() (
    cc: ControllerComponents,
    config: Configuration
)(using ec: scala.concurrent.ExecutionContext)
    extends AbstractController(cc)
    with Logging {

  private val random = new Random()

  // Configuration
  private val failureRate = config
    .getOptional[Double]("simulator.failureRate")
    .getOrElse(0.3) // 30% failure rate by default

  private val slowResponseRate = config
    .getOptional[Double]("simulator.slowResponseRate")
    .getOrElse(0.2) // 20% slow responses

  private val slowResponseDelayMs = config
    .getOptional[Int]("simulator.slowResponseDelayMs")
    .getOrElse(2000) // 2 second delay

  /** Generic webhook endpoint that randomly fails.
    *
    * Possible responses:
    * - 200 OK (success)
    * - 400 Bad Request (non-retryable)
    * - 429 Too Many Requests (retryable with Retry-After header)
    * - 500 Internal Server Error (retryable)
    * - 503 Service Unavailable (retryable with Retry-After header)
    * - Slow response (tests timeouts)
    */
  def webhook(path: String): Action[JsValue] = Action.async(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val eventType   = request.headers.get("X-Event-Type").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(
      s"[WEBHOOK] Received event: $eventType (id=$eventId, aggregate=$aggregateId) -> /$path"
    )
    logger.debug(s"[WEBHOOK] Headers: ${request.headers.toSimpleMap}")
    logger.debug(s"[WEBHOOK] Payload: ${request.body}")

    // Simulate slow response
    if (random.nextDouble() < slowResponseRate) {
      logger.warn(
        s"[WEBHOOK] Simulating slow response ($slowResponseDelayMs ms) for event $eventId"
      )
      Thread.sleep(slowResponseDelayMs)
    }

    // Simulate random failures
    val roll = random.nextDouble()

    val response = if (roll < failureRate) {
      // Failure scenarios
      val failureType = random.nextDouble()

      if (failureType < 0.25) {
        // 25% of failures: 400 Bad Request (non-retryable)
        logger.error(s"[WEBHOOK] Simulating 400 Bad Request for event $eventId")
        BadRequest(
          Json.obj(
            "error" -> "Invalid payload format",
            "eventId" -> eventId,
            "message" -> "This is a simulated non-retryable error"
          )
        )
      } else if (failureType < 0.5) {
        // 25% of failures: 429 Too Many Requests (retryable with Retry-After)
        val retryAfterSeconds = 3
        logger.error(
          s"[WEBHOOK] Simulating 429 Too Many Requests for event $eventId (Retry-After: $retryAfterSeconds)"
        )
        TooManyRequests(
          Json.obj(
            "error" -> "Rate limit exceeded",
            "eventId" -> eventId,
            "message" -> "This is a simulated rate limiting error",
            "retryAfter" -> retryAfterSeconds
          )
        ).withHeaders("Retry-After" -> retryAfterSeconds.toString)
      } else if (failureType < 0.75) {
        // 25% of failures: 500 Internal Server Error (retryable)
        logger.error(s"[WEBHOOK] Simulating 500 Internal Server Error for event $eventId")
        InternalServerError(
          Json.obj(
            "error" -> "Internal processing error",
            "eventId" -> eventId,
            "message" -> "This is a simulated retryable error"
          )
        )
      } else {
        // 25% of failures: 503 Service Unavailable (retryable with Retry-After)
        val retryAfterSeconds = 5
        logger.error(
          s"[WEBHOOK] Simulating 503 Service Unavailable for event $eventId (Retry-After: $retryAfterSeconds)"
        )
        ServiceUnavailable(
          Json.obj(
            "error" -> "Service temporarily unavailable",
            "eventId" -> eventId,
            "message" -> "This is a simulated retryable error",
            "retryAfter" -> retryAfterSeconds
          )
        ).withHeaders("Retry-After" -> retryAfterSeconds.toString)
      }
    } else {
      // Success
      logger.info(s"[WEBHOOK] Successfully processed event $eventId")
      Ok(
        Json.obj(
          "status" -> "success",
          "eventId" -> eventId,
          "eventType" -> eventType,
          "aggregateId" -> aggregateId,
          "receivedAt" -> System.currentTimeMillis()
        )
      )
    }

    scala.concurrent.Future.successful(response)
  }

  /** Always succeeds - useful for testing success scenarios.
    * Returns billing-specific response when called for billing destinations.
    */
  def alwaysSucceed: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")
    val destination = request.headers.get("X-Destination").getOrElse("")

    logger.info(s"[WEBHOOK-SUCCESS] Received event $eventId for destination: $destination")

    // If this is a billing request, return proper transaction response
    if (destination == "billing") {
      val transactionId = s"TXN-${aggregateId}-${System.currentTimeMillis()}"

      // Extract amount and currency from request payload
      val amount   = (request.body \ "amount").asOpt[BigDecimal].getOrElse(BigDecimal(0))
      val currency = (request.body \ "currency").asOpt[String].getOrElse("USD")

      logger.info(
        s"[BILLING-SUCCESS] Transaction $transactionId created for amount $amount $currency"
      )

      Ok(
        Json.obj(
          "status" -> "success",
          "transaction" -> Json.obj(
            "id" -> transactionId,
            "amount" -> amount,
            "currency" -> currency,
            "status" -> "completed",
            "processedAt" -> System.currentTimeMillis()
          ),
          "message" -> "Payment processed successfully"
        )
      )
    } else {
      // Generic success response for other destinations
      Ok(
        Json.obj(
          "status" -> "success",
          "eventId" -> eventId,
          "message" -> "Always succeeds"
        )
      )
    }
  }

  /** Always fails with 500 - useful for testing retry logic.
    */
  def alwaysFail: Action[JsValue] = Action(parse.json) { request =>
    val eventId = request.headers.get("X-Event-Id").getOrElse("unknown")
    logger.error(s"[WEBHOOK-FAIL] Received event $eventId - returning 500")

    InternalServerError(
      Json.obj(
        "error" -> "Simulated failure",
        "eventId" -> eventId,
        "message" -> "This endpoint always fails"
      )
    )
  }

  /** Always returns 400 - useful for testing non-retryable errors.
    */
  def alwaysBadRequest: Action[JsValue] = Action(parse.json) { request =>
    val eventId = request.headers.get("X-Event-Id").getOrElse("unknown")
    logger.error(s"[WEBHOOK-BAD-REQUEST] Received event $eventId - returning 400")

    BadRequest(
      Json.obj(
        "error" -> "Invalid request",
        "eventId" -> eventId,
        "message" -> "This endpoint always returns 400"
      )
    )
  }

  /** Always returns 429 Too Many Requests - useful for testing rate limiting with Retry-After.
    */
  def alwaysRateLimited: Action[JsValue] = Action(parse.json) { request =>
    val eventId           = request.headers.get("X-Event-Id").getOrElse("unknown")
    val retryAfterSeconds = 3
    logger.error(
      s"[WEBHOOK-RATE-LIMITED] Received event $eventId - returning 429 (Retry-After: $retryAfterSeconds)"
    )

    TooManyRequests(
      Json.obj(
        "error" -> "Rate limit exceeded",
        "eventId" -> eventId,
        "message" -> "This endpoint always returns 429",
        "retryAfter" -> retryAfterSeconds
      )
    ).withHeaders("Retry-After" -> retryAfterSeconds.toString)
  }

  /** Always returns 503 Service Unavailable - useful for testing service unavailability with Retry-After.
    */
  def alwaysUnavailable: Action[JsValue] = Action(parse.json) { request =>
    val eventId           = request.headers.get("X-Event-Id").getOrElse("unknown")
    val retryAfterSeconds = 5
    logger.error(
      s"[WEBHOOK-UNAVAILABLE] Received event $eventId - returning 503 (Retry-After: $retryAfterSeconds)"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "Service unavailable",
        "eventId" -> eventId,
        "message" -> "This endpoint always returns 503",
        "retryAfter" -> retryAfterSeconds
      )
    ).withHeaders("Retry-After" -> retryAfterSeconds.toString)
  }

  /** Returns stats about received webhooks.
    */
  def stats: Action[AnyContent] = Action {
    Ok(
      Json.obj(
        "configuration" -> Json.obj(
          "failureRate" -> failureRate,
          "slowResponseRate" -> slowResponseRate,
          "slowResponseDelayMs" -> slowResponseDelayMs
        ),
        "message" -> "Webhook simulator is running"
      )
    )
  }

  /** Inventory webhook that returns a reservationId in the response.
    * This ID can be extracted using JSONPath: $.reservationId
    */
  def inventoryWebhook: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[INVENTORY-WEBHOOK] Received event $eventId for aggregate $aggregateId")
    logger.debug(s"[INVENTORY-WEBHOOK] Payload: ${request.body}")

    // Generate a unique reservation ID
    val reservationId = s"RES-${aggregateId}-${System.currentTimeMillis()}"
    val warehouseId = "WH-NYC-01"

    // Extract payload details
    val orderId = (request.body \ "orderId").asOpt[Long].getOrElse(0L)
    val items = (request.body \ "items").asOpt[JsArray].getOrElse(Json.arr())
    val shippingType = (request.body \ "shippingType").asOpt[String].getOrElse("domestic")

    // Calculate expiry time (15 minutes from now)
    val expiresAt = System.currentTimeMillis() + (15 * 60 * 1000)

    logger.info(s"[INVENTORY-WEBHOOK] Reserved inventory: $reservationId, warehouse: $warehouseId, shippingType: $shippingType")

    Ok(
      Json.obj(
        "reservationId" -> reservationId,
        "orderId" -> orderId,
        "warehouseId" -> warehouseId,
        "warehouse" -> Json.obj(
          "id" -> warehouseId,
          "name" -> "NYC Distribution Center",
          "location" -> Json.obj(
            "city" -> "New York",
            "state" -> "NY",
            "country" -> "US"
          )
        ),
        "items" -> items.as[Seq[JsObject]].map { item =>
          Json.obj(
            "sku" -> (item \ "sku").asOpt[String].getOrElse("UNKNOWN"),
            "quantity" -> (item \ "quantity").asOpt[Int].getOrElse(1),
            "reserved" -> true,
            "binLocation" -> s"A${random.nextInt(20)}-${random.nextInt(50)}"
          )
        },
        "shippingType" -> shippingType,
        "status" -> "reserved",
        "expiresAt" -> expiresAt,
        "reservedAt" -> System.currentTimeMillis()
      )
    )
  }

  /** Inventory revert endpoint - cancels inventory reservation.
    * URL pattern: /simulator/webhook/inventory/revert/:inventoryId
    */
  def inventoryRevert(inventoryId: String): Action[AnyContent] = Action { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType   = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination = request.headers.get("X-Revert-Destination").getOrElse("unknown")

    logger.warn(
      s"[INVENTORY-REVERT] Reverting inventory reservation: $inventoryId " +
        s"(aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    Ok(
      Json.obj(
        "status" -> "reverted",
        "message" -> "Inventory reservation cancelled",
        "inventoryId" -> inventoryId,
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }

  /** Shipping webhook that returns a shippingId in the response.
    * This ID can be extracted using JSONPath: $.shipping.id
    */
  def shippingWebhook: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[SHIPPING-WEBHOOK] Received event $eventId for aggregate $aggregateId")
    logger.debug(s"[SHIPPING-WEBHOOK] Payload: ${request.body}")

    // Generate a unique shipping ID
    val shippingId = s"SHIP-${aggregateId}-${System.currentTimeMillis()}"

    Ok(
      Json.obj(
        "status" -> "success",
        "message" -> "Shipment created",
        "shipping" -> Json.obj(
          "id" -> shippingId,
          "trackingNumber" -> s"TRK${random.nextInt(999999)}",
          "carrier" -> "YamatoTransport",
          "estimatedDelivery" -> "2025-11-05"
        ),
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }

  /** Shipping revert endpoint - cancels shipment.
    * URL pattern: /simulator/webhook/shipping/:shippingId/cancel
    */
  def shippingRevert(shippingId: String): Action[AnyContent] = Action { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType   = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination = request.headers.get("X-Revert-Destination").getOrElse("unknown")

    logger.warn(
      s"[SHIPPING-REVERT] Cancelling shipment: $shippingId " +
        s"(aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    // 50% chance of failure to test DLQ retry mechanism
    if (random.nextDouble() < 0.5) {
      logger.error(s"[SHIPPING-REVERT] Simulating 503 Service Unavailable for revert of $shippingId")
      ServiceUnavailable(
        Json.obj(
          "error" -> "Shipment cancellation service temporarily unavailable",
          "shippingId" -> shippingId,
          "aggregateId" -> aggregateId,
          "message" -> "This is a simulated retryable error for testing DLQ retry"
        )
      ).withHeaders("Retry-After" -> "2")
    } else {
      Ok(
        Json.obj(
          "status" -> "cancelled",
          "message" -> "Shipment cancelled",
          "shippingId" -> shippingId,
          "aggregateId" -> aggregateId,
          "refundAmount" -> 500,
          "timestamp" -> System.currentTimeMillis()
        )
      )
    }
  }

  /** Billing webhook that returns a transaction ID in the response.
    * This ID can be extracted using JSONPath: $.transaction.id
    */
  def billingWebhook: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[BILLING-WEBHOOK] Received event $eventId for aggregate $aggregateId")
    logger.debug(s"[BILLING-WEBHOOK] Payload: ${request.body}")

    // Generate a unique transaction ID
    val transactionId = s"TXN-${aggregateId}-${System.currentTimeMillis()}"

    // Extract amount and currency from request payload
    val amount   = (request.body \ "amount").asOpt[BigDecimal].getOrElse(BigDecimal(100.00))
    val currency = (request.body \ "currency").asOpt[String].getOrElse("USD")

    logger.info(
      s"[BILLING-WEBHOOK] Transaction $transactionId created for amount $amount $currency"
    )

    Ok(
      Json.obj(
        "status" -> "success",
        "message" -> "Payment processed successfully",
        "transaction" -> Json.obj(
          "id" -> transactionId,
          "amount" -> amount,
          "currency" -> currency,
          "status" -> "completed",
          "processedAt" -> System.currentTimeMillis()
        ),
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }

  /** Domestic shipping webhook - for local deliveries.
    */
  def domesticShippingWebhook: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[DOMESTIC-SHIPPING-WEBHOOK] Received event $eventId for aggregate $aggregateId")
    logger.debug(s"[DOMESTIC-SHIPPING-WEBHOOK] Payload: ${request.body}")

    val shipmentId  = s"SHIP-DOM-${aggregateId}-${System.currentTimeMillis()}"
    val orderId = (request.body \ "orderId").asOpt[Long].getOrElse(0L)
    val customerId  = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
    val address = (request.body \ "address").asOpt[JsObject].getOrElse(Json.obj())

    // Calculate estimated delivery (2 business days)
    val estimatedDelivery = System.currentTimeMillis() + (2 * 24 * 60 * 60 * 1000)

    logger.info(s"[DOMESTIC-SHIPPING-WEBHOOK] Created shipment: $shipmentId for order $orderId")

    Ok(
      Json.obj(
        "shipmentId" -> shipmentId,
        "orderId" -> orderId,
        "customerId" -> customerId,
        "carrier" -> Json.obj(
          "code" -> "YAMATO",
          "name" -> "Yamato Transport",
          "service" -> "Express"
        ),
        "trackingNumber" -> s"9400${random.nextInt(100000000)}",
        "shippingAddress" -> address,
        "cost" -> Json.obj(
          "amount" -> 12.50,
          "currency" -> "USD"
        ),
        "status" -> "confirmed",
        "estimatedDelivery" -> estimatedDelivery,
        "createdAt" -> System.currentTimeMillis()
      )
    )
  }

  /** International shipping webhook - for cross-border deliveries.
    */
  def internationalShippingWebhook: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(
      s"[INTERNATIONAL-SHIPPING-WEBHOOK] Received event $eventId for aggregate $aggregateId"
    )
    logger.debug(s"[INTERNATIONAL-SHIPPING-WEBHOOK] Payload: ${request.body}")

    val shipmentId  = s"SHIP-INTL-${aggregateId}-${System.currentTimeMillis()}"
    val orderId = (request.body \ "orderId").asOpt[Long].getOrElse(0L)
    val customerId  = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
    val address = (request.body \ "address").asOpt[JsObject].getOrElse(Json.obj())

    // Calculate estimated delivery (7 business days for international)
    val estimatedDelivery = System.currentTimeMillis() + (7 * 24 * 60 * 60 * 1000)

    logger.info(s"[INTERNATIONAL-SHIPPING-WEBHOOK] Created shipment: $shipmentId for order $orderId")

    Ok(
      Json.obj(
        "shipmentId" -> shipmentId,
        "orderId" -> orderId,
        "customerId" -> customerId,
        "carrier" -> Json.obj(
          "code" -> "DHL",
          "name" -> "DHL Express",
          "service" -> "International Priority"
        ),
        "trackingNumber" -> s"1Z999AA1${random.nextInt(1000000000)}",
        "shippingAddress" -> address,
        "cost" -> Json.obj(
          "amount" -> 45.00,
          "currency" -> "USD"
        ),
        "customs" -> Json.obj(
          "declaration" -> true,
          "reference" -> s"CUST-${System.currentTimeMillis()}",
          "incoterms" -> "DDP"
        ),
        "insurance" -> Json.obj(
          "required" -> true,
          "value" -> 500.00,
          "currency" -> "USD"
        ),
        "status" -> "confirmed",
        "estimatedDelivery" -> estimatedDelivery,
        "createdAt" -> System.currentTimeMillis()
      )
    )
  }

  /** Domestic shipping revert endpoint.
    */
  def domesticShippingRevert(shippingId: String): Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val reason      = (request.body \ "reason").asOpt[String].getOrElse("unknown")
    val customerId  = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
    val refundAmt   = (request.body \ "refundAmount").asOpt[BigDecimal].getOrElse(BigDecimal(0))

    logger.warn(
      s"[DOMESTIC-SHIPPING-REVERT] Cancelling domestic shipment: $shippingId " +
        s"(aggregate=$aggregateId, reason=$reason, customer=$customerId, refund=$refundAmt)"
    )

    // 80% chance of failure to test DLQ retry mechanism
    if (random.nextDouble() < 0.8) {
      logger.error(s"[DOMESTIC-SHIPPING-REVERT] Simulating 503 Service Unavailable for revert of $shippingId")
      ServiceUnavailable(
        Json.obj(
          "error" -> "Shipment cancellation service temporarily unavailable",
          "shippingId" -> shippingId,
          "aggregateId" -> aggregateId,
          "message" -> "This is a simulated retryable error for testing DLQ retry"
        )
      ).withHeaders("Retry-After" -> "2")
    } else {
      Ok(
        Json.obj(
          "status" -> "cancelled",
          "message" -> "Domestic shipment cancelled",
          "shippingId" -> shippingId,
          "timestamp" -> System.currentTimeMillis()
        )
      )
    }
  }

  /** International shipping revert endpoint.
    */
  def internationalShippingRevert(shippingId: String): Action[JsValue] = Action(parse.json) {
    request =>
      val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
      val reason      = (request.body \ "reason").asOpt[String].getOrElse("unknown")
      val customerId  = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
      val refundAmt   = (request.body \ "refundAmount").asOpt[BigDecimal].getOrElse(BigDecimal(0))

      logger.warn(
        s"[INTERNATIONAL-SHIPPING-REVERT] Cancelling international shipment: $shippingId " +
          s"(aggregate=$aggregateId, reason=$reason, customer=$customerId, refund=$refundAmt)"
      )

      Ok(
        Json.obj(
          "status" -> "cancelled",
          "message" -> "International shipment cancelled (customs clearance will be aborted)",
          "shippingId" -> shippingId,
          "timestamp" -> System.currentTimeMillis()
        )
      )
  }

  /** Billing revert endpoint - processes refund.
    * URL pattern: /simulator/webhook/billing/:transactionId/refund
    */
  def billingRevert(transactionId: String): Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType   = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination = request.headers.get("X-Revert-Destination").getOrElse("unknown")

    // Extract refund details from request payload
    val amount   = (request.body \ "amount").asOpt[BigDecimal].getOrElse(BigDecimal(0))
    val currency = (request.body \ "currency").asOpt[String].getOrElse("USD")
    val reason   = (request.body \ "reason").asOpt[String].getOrElse("unknown")

    logger.warn(
      s"[BILLING-REVERT] Processing refund for transaction: $transactionId " +
        s"(amount=$amount $currency, reason=$reason, aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    Ok(
      Json.obj(
        "status" -> "refunded",
        "message" -> "Transaction refunded successfully",
        "transactionId" -> transactionId,
        "refund" -> Json.obj(
          "refundId" -> s"RFD-${transactionId}-${System.currentTimeMillis()}",
          "amount" -> amount,
          "currency" -> currency,
          "reason" -> reason,
          "processedAt" -> System.currentTimeMillis()
        ),
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }

  /** Email revert endpoint - cancels email notification.
    * URL pattern: /simulator/webhook/email/:emailId/cancel
    */
  def emailRevert(emailId: String): Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType   = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination = request.headers.get("X-Revert-Destination").getOrElse("unknown")

    logger.warn(
      s"[EMAIL-REVERT] Cancelling email: $emailId " +
        s"(aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    Ok(
      Json.obj(
        "status" -> "cancelled",
        "message" -> "Email notification cancelled",
        "emailId" -> emailId,
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }

  /** Simulates GitHub API - Returns 429 with X-RateLimit-Reset header (Unix timestamp).
    */
  def githubRateLimited: Action[AnyContent] = Action { request =>
    val resetTime = (System.currentTimeMillis() / 1000) + 30 // 30 seconds from now

    logger.warn(s"[GITHUB-API] Rate limited - X-RateLimit-Reset: $resetTime")

    TooManyRequests(
      Json.obj(
        "message" -> "API rate limit exceeded",
        "documentation_url" -> "https://docs.github.com/rest/overview/resources-in-the-rest-api#rate-limiting"
      )
    ).withHeaders(
      "X-RateLimit-Limit" -> "60",
      "X-RateLimit-Remaining" -> "0",
      "X-RateLimit-Reset" -> resetTime.toString, // Unix epoch seconds
      "X-RateLimit-Resource" -> "core"
    )
  }

  /** Simulates Stripe API - Returns 429 with standard Retry-After header (seconds).
    */
  def stripeRateLimited: Action[JsValue] = Action(parse.json) { request =>
    val retryAfterSeconds = 15

    logger.warn(s"[STRIPE-API] Rate limited - Retry-After: $retryAfterSeconds seconds")

    TooManyRequests(
      Json.obj(
        "error" -> Json.obj(
          "type" -> "rate_limit_error",
          "message" -> "Too many requests hit the API too quickly. We recommend an exponential backoff of your requests."
        )
      )
    ).withHeaders(
      "Retry-After" -> retryAfterSeconds.toString // Seconds to wait
    )
  }

  /** Simulates custom API - Returns 503 with retry info in JSON body.
    */
  def customApiUnavailable: Action[JsValue] = Action(parse.json) { request =>
    val retryAfterSeconds = 20

    logger.warn(s"[CUSTOM-API] Service unavailable - retry info in body")

    ServiceUnavailable(
      Json.obj(
        "status" -> "unavailable",
        "message" -> "Service temporarily unavailable due to maintenance",
        "rateLimit" -> Json.obj(
          "retryAfter" -> retryAfterSeconds, // Seconds (for JSONPath extraction)
          "resetAt" -> (System.currentTimeMillis() + retryAfterSeconds * 1000)
        )
      )
    )
  }

  /** Fraud checks endpoint - returns risk score for conditional routing.
    */
  def fraudCheck: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    // Simulate rate limiting with 20% chance
    // This demonstrates Retry-After header handling while ensuring retries eventually succeed
    val shouldRateLimit = random.nextDouble() < 0.2

    if (shouldRateLimit) {
      val retryAfterSeconds = 10

      logger.warn(
        s"[FRAUD-CHECK] Rate limited - Event $eventId for aggregate $aggregateId - Retry-After: $retryAfterSeconds seconds"
      )

      TooManyRequests(
        Json.obj(
          "error" -> "rate_limit_exceeded",
          "message" -> "Fraud check API rate limit exceeded. Please retry after the specified delay.",
          "retryAfter" -> retryAfterSeconds,
          "aggregateId" -> aggregateId
        )
      ).withHeaders(
        "Retry-After" -> retryAfterSeconds.toString // Standard HTTP header (seconds)
      )
    } else {
      // Calculate risk score based on aggregate ID (for demo)
      // Even IDs = low risk (< 50), Odd IDs = high risk (>= 50)
      val riskScore = if (aggregateId.toLongOption.exists(_ % 2 == 0)) 25 else 75

      logger.info(s"[FRAUD-CHECK] Event $eventId for aggregate $aggregateId - riskScore: $riskScore")

      Ok(
        Json.obj(
          "status" -> "success",
          "riskScore" -> riskScore,
          "riskLevel" -> (if (riskScore < 50) "low" else "high"),
          "checks" -> Json.obj(
            "addressVerification" -> "passed",
            "cvvCheck" -> "passed",
            "velocityCheck" -> (if (riskScore < 50) "passed" else "flagged")
          ),
          "timestamp" -> System.currentTimeMillis()
        )
      )
    }
  }

  /** High-value processing endpoint - for low-risk high-value orders.
    */
  def highValueProcessing: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[HIGH-VALUE-PROCESSING] Received event $eventId for aggregate $aggregateId")

    Ok(
      Json.obj(
        "status" -> "success",
        "message" -> "High-value order processing completed",
        "processingId" -> s"HV-${aggregateId}-${System.currentTimeMillis()}",
        "additionalVerification" -> "completed",
        "insuranceAdded" -> true,
        "signatureRequired" -> true,
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }
}
