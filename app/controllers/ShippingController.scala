package controllers

import play.api.{ Configuration, Logging }
import play.api.libs.json.*
import play.api.mvc.*

import javax.inject.*
import scala.util.Random

/** Shipping service controller - handles domestic and international shipping.
  *
  * Endpoints:
  * - POST /api/domestic-shipping - Create domestic shipment
  * - POST /api/domestic-shipping/:id/cancel - Cancel domestic shipment
  * - POST /api/international-shipping - Create international shipment
  * - POST /api/international-shipping/:id/cancel - Cancel international shipment
  */
@Singleton
class ShippingController @Inject() (cc: ControllerComponents, config: Configuration)(
    using ec: scala.concurrent.ExecutionContext
) extends AbstractController(cc)
    with Logging {

  private val random = new Random()

  // Read failure rates from configuration
  private val domesticCreateFailureRate =
    config.get[Double]("service.failure.rates.shipping.domestic.create")
  private val domesticCancelFailureRate =
    config.get[Double]("service.failure.rates.shipping.domestic.cancel")
  private val internationalCreateFailureRate =
    config.get[Double]("service.failure.rates.shipping.international.create")
  private val internationalCancelFailureRate =
    config.get[Double]("service.failure.rates.shipping.international.cancel")

  /** Create domestic shipment.
    * Returns a shipmentId that can be used for cancellation.
    * Configurable failure rate from application.conf (service.failure.rates.shipping.*).
    */
  def createDomesticShipment: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[DOMESTIC-SHIPPING] Create shipment - event=$eventId, aggregate=$aggregateId")
    logger.debug(s"[DOMESTIC-SHIPPING] Payload: ${request.body}")

    // Simulate shipping service failure based on configured rate
    if (random.nextDouble() < domesticCreateFailureRate) {
      logger.error(
        s"[DOMESTIC-SHIPPING] Simulating shipping service failure - event=$eventId, aggregate=$aggregateId"
      )
      ServiceUnavailable(
        Json.obj(
          "error" -> "shipping_service_unavailable",
          "message" -> "Domestic shipping service is temporarily unavailable",
          "aggregateId" -> aggregateId,
          "timestamp" -> System.currentTimeMillis()
        )
      ).withHeaders("Retry-After" -> "3")
    } else {
      val shipmentId = s"SHIP-DOM-${aggregateId}-${System.currentTimeMillis()}"
      val orderId    = (request.body \ "orderId").asOpt[Long].getOrElse(0L)
      val customerId = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
      val address    = (request.body \ "address").asOpt[JsObject].getOrElse(Json.obj())

      // Calculate estimated delivery (2 business days)
      val estimatedDelivery = System.currentTimeMillis() + (2 * 24 * 60 * 60 * 1000)

      logger.info(s"[DOMESTIC-SHIPPING] Created shipment: $shipmentId for order $orderId")

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
          "cost" -> Json.obj("amount" -> 12.50, "currency" -> "USD"),
          "status" -> "confirmed",
          "estimatedDelivery" -> estimatedDelivery,
          "createdAt" -> System.currentTimeMillis()
        )
      )
    }
  }

  /** Create domestic shipment - ALWAYS SUCCEEDS.
    */
  def createDomesticShipmentSucceed: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(
      s"[DOMESTIC-SHIPPING-SUCCEED] Create shipment - event=$eventId, aggregate=$aggregateId"
    )

    val shipmentId = s"SHIP-DOM-${aggregateId}-${System.currentTimeMillis()}"
    val orderId    = (request.body \ "orderId").asOpt[Long].getOrElse(0L)
    val customerId = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
    val address    = (request.body \ "address").asOpt[JsObject].getOrElse(Json.obj())

    val estimatedDelivery = System.currentTimeMillis() + (2 * 24 * 60 * 60 * 1000)

    logger.info(s"[DOMESTIC-SHIPPING-SUCCEED] Created shipment: $shipmentId for order $orderId")

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
        "cost" -> Json.obj("amount" -> 12.50, "currency" -> "USD"),
        "status" -> "confirmed",
        "estimatedDelivery" -> estimatedDelivery,
        "createdAt" -> System.currentTimeMillis()
      )
    )
  }

  /** Create domestic shipment - ALWAYS FAILS.
    */
  def createDomesticShipmentFail: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.error(
      s"[DOMESTIC-SHIPPING-FAIL] Simulating shipping failure - event=$eventId, aggregate=$aggregateId"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "shipping_service_unavailable",
        "message" -> "Domestic shipping service is temporarily unavailable",
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    ).withHeaders("Retry-After" -> "3")
  }

  /** Cancel domestic shipment (revert endpoint).
    * Configurable failure rate from application.conf (service.failure.rates.shipping.*).
    */
  def cancelDomesticShipment(shipmentId: String): Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val reason      = (request.body \ "reason").asOpt[String].getOrElse("unknown")
    val customerId  = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
    val orderId     = (request.body \ "orderId").asOpt[String].getOrElse("unknown")

    logger.warn(
      s"[DOMESTIC-SHIPPING-REVERT] Cancelling shipment: $shipmentId " +
        s"(aggregate=$aggregateId, reason=$reason, customer=$customerId, order=$orderId)"
    )

    // Simulate cancellation service failure based on configured rate
    if (random.nextDouble() < domesticCancelFailureRate) {
      logger.error(
        s"[DOMESTIC-SHIPPING-REVERT] Simulating 503 Service Unavailable for revert of $shipmentId"
      )
      ServiceUnavailable(
        Json.obj(
          "error" -> "Shipment cancellation service temporarily unavailable",
          "shipmentId" -> shipmentId,
          "aggregateId" -> aggregateId,
          "message" -> "This is a simulated retryable error for testing DLQ retry"
        )
      ).withHeaders("Retry-After" -> "2")
    } else {
      Ok(
        Json.obj(
          "status" -> "cancelled",
          "message" -> "Domestic shipment cancelled",
          "shipmentId" -> shipmentId,
          "timestamp" -> System.currentTimeMillis()
        )
      )
    }
  }

  /** Cancel domestic shipment - ALWAYS SUCCEEDS (revert endpoint).
    */
  def cancelDomesticShipmentSucceed: Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val reason      = (request.body \ "reason").asOpt[String].getOrElse("unknown")
    val customerId  = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
    val orderId     = (request.body \ "orderId").asOpt[String].getOrElse("unknown")
    val shipmentId  = s"SHIP-DOM-${aggregateId}-TEST"

    logger.warn(
      s"[DOMESTIC-SHIPPING-REVERT-SUCCEED] Cancelling shipment: $shipmentId " +
        s"(aggregate=$aggregateId, reason=$reason, customer=$customerId, order=$orderId)"
    )

    Ok(
      Json.obj(
        "status" -> "cancelled",
        "message" -> "Domestic shipment cancelled",
        "shipmentId" -> shipmentId,
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }

  /** Cancel domestic shipment - ALWAYS FAILS (revert endpoint).
    */
  def cancelDomesticShipmentFail: Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val reason      = (request.body \ "reason").asOpt[String].getOrElse("unknown")
    val customerId  = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
    val orderId     = (request.body \ "orderId").asOpt[String].getOrElse("unknown")

    logger.error(
      s"[DOMESTIC-SHIPPING-REVERT-FAIL] Simulating cancellation failure " +
        s"(aggregate=$aggregateId, reason=$reason, customer=$customerId, order=$orderId)"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "Shipment cancellation service temporarily unavailable",
        "aggregateId" -> aggregateId,
        "message" -> "Simulated retryable error for testing DLQ retry"
      )
    ).withHeaders("Retry-After" -> "2")
  }

  /** Create international shipment.
    * Returns a shipmentId that can be used for cancellation.
    * Configurable failure rate from application.conf (service.failure.rates.shipping.*).
    */
  def createInternationalShipment: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(
      s"[INTERNATIONAL-SHIPPING] Create shipment - event=$eventId, aggregate=$aggregateId"
    )
    logger.debug(s"[INTERNATIONAL-SHIPPING] Payload: ${request.body}")

    // Simulate international shipping service failure based on configured rate
    if (random.nextDouble() < internationalCreateFailureRate) {
      logger.error(
        s"[INTERNATIONAL-SHIPPING] Simulating shipping service failure - event=$eventId, aggregate=$aggregateId"
      )
      ServiceUnavailable(
        Json.obj(
          "error" -> "international_shipping_unavailable",
          "message" -> "International shipping service is temporarily unavailable",
          "aggregateId" -> aggregateId,
          "timestamp" -> System.currentTimeMillis()
        )
      ).withHeaders("Retry-After" -> "3")
    } else {
      val shipmentId = s"SHIP-INTL-${aggregateId}-${System.currentTimeMillis()}"
      val orderId    = (request.body \ "orderId").asOpt[Long].getOrElse(0L)
      val customerId = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
      val address    = (request.body \ "address").asOpt[JsObject].getOrElse(Json.obj())

      // Calculate estimated delivery (7 business days for international)
      val estimatedDelivery = System.currentTimeMillis() + (7 * 24 * 60 * 60 * 1000)

      logger.info(s"[INTERNATIONAL-SHIPPING] Created shipment: $shipmentId for order $orderId")

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
          "cost" -> Json.obj("amount" -> 45.00, "currency" -> "USD"),
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
  }

  /** Create international shipment - ALWAYS SUCCEEDS.
    */
  def createInternationalShipmentSucceed: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(
      s"[INTERNATIONAL-SHIPPING-SUCCEED] Create shipment - event=$eventId, aggregate=$aggregateId"
    )

    val shipmentId = s"SHIP-INTL-${aggregateId}-${System.currentTimeMillis()}"
    val orderId    = (request.body \ "orderId").asOpt[Long].getOrElse(0L)
    val customerId = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
    val address    = (request.body \ "address").asOpt[JsObject].getOrElse(Json.obj())

    val estimatedDelivery = System.currentTimeMillis() + (7 * 24 * 60 * 60 * 1000)

    logger.info(
      s"[INTERNATIONAL-SHIPPING-SUCCEED] Created shipment: $shipmentId for order $orderId"
    )

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
        "cost" -> Json.obj("amount" -> 45.00, "currency" -> "USD"),
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

  /** Create international shipment - ALWAYS FAILS.
    */
  def createInternationalShipmentFail: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.error(
      s"[INTERNATIONAL-SHIPPING-FAIL] Simulating shipping failure - event=$eventId, aggregate=$aggregateId"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "international_shipping_unavailable",
        "message" -> "International shipping service is temporarily unavailable",
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    ).withHeaders("Retry-After" -> "3")
  }

  /** Cancel international shipment (revert endpoint).
    * Configurable failure rate from application.conf (service.failure.rates.shipping.*).
    */
  def cancelInternationalShipment(shipmentId: String): Action[JsValue] = Action(parse.json) {
    request =>
      val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
      val reason      = (request.body \ "reason").asOpt[String].getOrElse("unknown")
      val customerId  = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
      val orderId     = (request.body \ "orderId").asOpt[String].getOrElse("unknown")

      logger.warn(
        s"[INTERNATIONAL-SHIPPING-REVERT] Cancelling shipment: $shipmentId " +
          s"(aggregate=$aggregateId, reason=$reason, customer=$customerId, order=$orderId)"
      )

      // Simulate cancellation service failure based on configured rate
      if (random.nextDouble() < internationalCancelFailureRate) {
        logger.error(
          s"[INTERNATIONAL-SHIPPING-REVERT] Simulating cancellation failure for shipment: $shipmentId"
        )
        ServiceUnavailable(
          Json.obj(
            "error" -> "International shipment cancellation service temporarily unavailable",
            "aggregateId" -> aggregateId,
            "message" -> "Simulated retryable error for testing DLQ retry"
          )
        ).withHeaders("Retry-After" -> "3")
      } else {
        Ok(
          Json.obj(
            "status" -> "cancelled",
            "message" -> "International shipment cancelled (customs clearance will be aborted)",
            "shipmentId" -> shipmentId,
            "timestamp" -> System.currentTimeMillis()
          )
        )
      }
  }

  /** Cancel international shipment - ALWAYS SUCCEEDS (revert endpoint).
    */
  def cancelInternationalShipmentSucceed: Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val reason      = (request.body \ "reason").asOpt[String].getOrElse("unknown")
    val customerId  = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
    val orderId     = (request.body \ "orderId").asOpt[String].getOrElse("unknown")
    val shipmentId  = s"SHIP-INTL-${aggregateId}-TEST"

    logger.warn(
      s"[INTERNATIONAL-SHIPPING-REVERT-SUCCEED] Cancelling shipment: $shipmentId " +
        s"(aggregate=$aggregateId, reason=$reason, customer=$customerId, order=$orderId)"
    )

    Ok(
      Json.obj(
        "status" -> "cancelled",
        "message" -> "International shipment cancelled (customs clearance will be aborted)",
        "shipmentId" -> shipmentId,
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }

  /** Cancel international shipment - ALWAYS FAILS (revert endpoint).
    */
  def cancelInternationalShipmentFail: Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val reason      = (request.body \ "reason").asOpt[String].getOrElse("unknown")
    val customerId  = (request.body \ "customerId").asOpt[String].getOrElse("unknown")
    val orderId     = (request.body \ "orderId").asOpt[String].getOrElse("unknown")

    logger.error(
      s"[INTERNATIONAL-SHIPPING-REVERT-FAIL] Simulating cancellation failure " +
        s"(aggregate=$aggregateId, reason=$reason, customer=$customerId, order=$orderId)"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "International shipment cancellation service temporarily unavailable",
        "aggregateId" -> aggregateId,
        "message" -> "Simulated retryable error for testing DLQ retry"
      )
    ).withHeaders("Retry-After" -> "3")
  }
}
