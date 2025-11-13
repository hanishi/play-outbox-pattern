package controllers

import play.api.{ Configuration, Logging }
import play.api.libs.json.*
import play.api.mvc.*

import javax.inject.*
import scala.util.Random

/** Billing service controller - handles payment processing and refunds.
  *
  * Endpoints:
  * - POST /api/billing - Process payment
  * - POST /api/billing/:id/refund - Refund a transaction
  */
@Singleton
class BillingController @Inject() (cc: ControllerComponents, config: Configuration)(
    using ec: scala.concurrent.ExecutionContext
) extends AbstractController(cc)
    with Logging {

  private val random            = new Random()
  private val chargeFailureRate = config.get[Double]("service.failure.rates.billing.charge")
  private val refundFailureRate = config.get[Double]("service.failure.rates.billing.refund")

  /** Process payment endpoint.
    * Returns a transaction ID in the response that can be used for refunds.
    * Configurable failure rate from application.conf (service.failure.rates.billing.charge).
    */
  def charge: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[BILLING] Charge request - event=$eventId, aggregate=$aggregateId")
    logger.debug(s"[BILLING] Payload: ${request.body}")

    // Simulate payment gateway failure based on configured rate
    if (random.nextDouble() < chargeFailureRate) {
      logger.error(
        s"[BILLING] Simulating payment gateway failure - event=$eventId, aggregate=$aggregateId"
      )
      ServiceUnavailable(
        Json.obj(
          "error" -> "payment_gateway_unavailable",
          "message" -> "Payment gateway is temporarily unavailable",
          "aggregateId" -> aggregateId,
          "timestamp" -> System.currentTimeMillis()
        )
      ).withHeaders("Retry-After" -> "5")
    } else {
      // Generate a unique transaction ID
      val transactionId = s"TXN-${aggregateId}-${System.currentTimeMillis()}"

      // Extract amount and currency from request payload
      val amount   = (request.body \ "amount").asOpt[BigDecimal].getOrElse(BigDecimal(100.00))
      val currency = (request.body \ "currency").asOpt[String].getOrElse("USD")

      logger.info(s"[BILLING] Transaction $transactionId created for amount $amount $currency")

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
  }

  /** Process payment endpoint - ALWAYS SUCCEEDS.
    * Returns a transaction ID in the response that can be used for refunds.
    */
  def chargeSucceed: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[BILLING-SUCCEED] Charge request - event=$eventId, aggregate=$aggregateId")

    val transactionId = s"TXN-${aggregateId}-${System.currentTimeMillis()}"
    val amount        = (request.body \ "amount").asOpt[BigDecimal].getOrElse(BigDecimal(100.00))
    val currency      = (request.body \ "currency").asOpt[String].getOrElse("USD")

    logger.info(
      s"[BILLING-SUCCEED] Transaction $transactionId created for amount $amount $currency"
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

  /** Process payment endpoint - ALWAYS FAILS.
    * Returns 503 Service Unavailable to test compensation flow.
    */
  def chargeFail: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.error(
      s"[BILLING-FAIL] Simulating payment failure - event=$eventId, aggregate=$aggregateId"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "payment_gateway_unavailable",
        "message" -> "Payment gateway is temporarily unavailable",
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    ).withHeaders("Retry-After" -> "5")
  }

  /** Refund transaction (revert endpoint).
    * URL pattern: /api/billing/:transactionId/refund
    * Configurable failure rate from application.conf (service.failure.rates.billing.refund).
    */
  def refund(transactionId: String): Action[JsValue] = Action(parse.json) { request =>
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

    // Simulate refund service failure based on configured rate
    if (random.nextDouble() < refundFailureRate) {
      logger.error(
        s"[BILLING-REVERT] Simulating refund service failure for transaction: $transactionId"
      )
      ServiceUnavailable(
        Json.obj(
          "error" -> "refund_service_unavailable",
          "message" -> "Refund service is temporarily unavailable",
          "aggregateId" -> aggregateId,
          "timestamp" -> System.currentTimeMillis()
        )
      ).withHeaders("Retry-After" -> "5")
    } else {
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
  }

  /** Refund transaction - ALWAYS SUCCEEDS (revert endpoint).
    */
  def refundSucceed: Action[JsValue] = Action(parse.json) { request =>
    val aggregateId   = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType     = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination   = request.headers.get("X-Revert-Destination").getOrElse("unknown")
    val transactionId = s"TXN-${aggregateId}-TEST"

    val amount   = (request.body \ "amount").asOpt[BigDecimal].getOrElse(BigDecimal(0))
    val currency = (request.body \ "currency").asOpt[String].getOrElse("USD")
    val reason   = (request.body \ "reason").asOpt[String].getOrElse("unknown")

    logger.warn(
      s"[BILLING-REVERT-SUCCEED] Processing refund for transaction: $transactionId " +
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

  /** Refund transaction - ALWAYS FAILS (revert endpoint).
    */
  def refundFail: Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType   = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination = request.headers.get("X-Revert-Destination").getOrElse("unknown")

    logger.error(
      s"[BILLING-REVERT-FAIL] Simulating refund failure " +
        s"(aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "refund_service_unavailable",
        "message" -> "Refund service is temporarily unavailable",
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    ).withHeaders("Retry-After" -> "5")
  }
}
