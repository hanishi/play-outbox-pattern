package controllers

import play.api.{ Configuration, Logging }
import play.api.libs.json.*
import play.api.mvc.*

import javax.inject.*
import scala.util.Random

/** High-value processing controller - handles high-risk, high-value orders.
  *
  * Endpoints:
  * - POST /api/high-value-processing - Process high-value order
  * - POST /api/high-value-processing/:id/cancel - Cancel high-value processing
  */
@Singleton
class HighValueProcessingController @Inject() (cc: ControllerComponents, config: Configuration)(
    using ec: scala.concurrent.ExecutionContext
) extends AbstractController(cc)
    with Logging {

  private val random = new Random()

  // Read failure rates from configuration
  private val processFailureRate = config.get[Double]("service.failure.rates.highValue.process")
  private val cancelFailureRate  = config.get[Double]("service.failure.rates.highValue.cancel")

  /** Process high-value order endpoint.
    * Returns a processingId that can be used for cancellation.
    * Configurable failure rate from application.conf (service.failure.rates.highValue.process).
    */
  def process: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[HIGH-VALUE-PROCESSING] Process request - event=$eventId, aggregate=$aggregateId")

    // Simulate high-value processing failure based on configured rate
    if (random.nextDouble() < processFailureRate) {
      logger.error(
        s"[HIGH-VALUE-PROCESSING] Simulating processing service failure - event=$eventId, aggregate=$aggregateId"
      )
      ServiceUnavailable(
        Json.obj(
          "error" -> "high_value_processing_unavailable",
          "message" -> "High-value processing service is temporarily unavailable",
          "aggregateId" -> aggregateId,
          "timestamp" -> System.currentTimeMillis()
        )
      ).withHeaders("Retry-After" -> "5")
    } else {
      // Extract amount and currency from request payload
      val amount   = (request.body \ "amount").asOpt[BigDecimal].getOrElse(BigDecimal(100.00))
      val currency = (request.body \ "currency").asOpt[String].getOrElse("USD")

      val processingId = s"HV-${aggregateId}-${System.currentTimeMillis()}"

      logger.info(
        s"[HIGH-VALUE-PROCESSING] Processing $processingId for amount $amount $currency"
      )

      Ok(
        Json.obj(
          "status" -> "success",
          "message" -> "High-value order processing completed",
          "processingId" -> processingId,
          "amount" -> amount,
          "currency" -> currency,
          "additionalVerification" -> "completed",
          "insuranceAdded" -> true,
          "signatureRequired" -> true,
          "timestamp" -> System.currentTimeMillis()
        )
      )
    }
  }

  /** Process high-value order endpoint - ALWAYS SUCCEEDS.
    */
  def processSucceed: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(
      s"[HIGH-VALUE-PROCESSING-SUCCEED] Process request - event=$eventId, aggregate=$aggregateId"
    )

    val amount       = (request.body \ "amount").asOpt[BigDecimal].getOrElse(BigDecimal(100.00))
    val currency     = (request.body \ "currency").asOpt[String].getOrElse("USD")
    val processingId = s"HV-${aggregateId}-${System.currentTimeMillis()}"

    logger.info(
      s"[HIGH-VALUE-PROCESSING-SUCCEED] Processing $processingId for amount $amount $currency"
    )

    Ok(
      Json.obj(
        "status" -> "success",
        "message" -> "High-value order processing completed",
        "processingId" -> processingId,
        "amount" -> amount,
        "currency" -> currency,
        "additionalVerification" -> "completed",
        "insuranceAdded" -> true,
        "signatureRequired" -> true,
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }

  /** Process high-value order endpoint - ALWAYS FAILS.
    */
  def processFail: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.error(
      s"[HIGH-VALUE-PROCESSING-FAIL] Simulating processing failure - event=$eventId, aggregate=$aggregateId"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "high_value_processing_unavailable",
        "message" -> "High-value processing service is temporarily unavailable",
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    ).withHeaders("Retry-After" -> "5")
  }

  /** Cancel high-value processing (revert endpoint).
    * Configurable failure rate from application.conf (service.failure.rates.highValue.cancel).
    */
  def cancel(processingId: String): Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType   = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination = request.headers.get("X-Revert-Destination").getOrElse("unknown")

    // Extract refund details from request payload
    val amount   = (request.body \ "amount").asOpt[BigDecimal].getOrElse(BigDecimal(0))
    val currency = (request.body \ "currency").asOpt[String].getOrElse("USD")
    val reason   = (request.body \ "reason").asOpt[String].getOrElse("unknown")

    logger.warn(
      s"[HIGH-VALUE-PROCESSING-REVERT] Cancelling processing: $processingId " +
        s"(amount=$amount $currency, reason=$reason, aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    // Simulate cancellation service failure based on configured rate
    if (random.nextDouble() < cancelFailureRate) {
      logger.error(
        s"[HIGH-VALUE-PROCESSING-REVERT] Simulating cancellation failure for processing: $processingId"
      )
      ServiceUnavailable(
        Json.obj(
          "error" -> "high_value_cancellation_unavailable",
          "message" -> "High-value processing cancellation service is temporarily unavailable",
          "aggregateId" -> aggregateId,
          "timestamp" -> System.currentTimeMillis()
        )
      ).withHeaders("Retry-After" -> "5")
    } else {
      Ok(
        Json.obj(
          "status" -> "cancelled",
          "message" -> "High-value processing cancelled successfully",
          "processingId" -> processingId,
          "refund" -> Json.obj(
            "refundId" -> s"RFD-${processingId}-${System.currentTimeMillis()}",
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

  /** Cancel high-value processing - ALWAYS SUCCEEDS (revert endpoint).
    */
  def cancelSucceed: Action[JsValue] = Action(parse.json) { request =>
    val aggregateId  = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType    = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination  = request.headers.get("X-Revert-Destination").getOrElse("unknown")
    val processingId = s"HV-${aggregateId}-TEST"

    val amount   = (request.body \ "amount").asOpt[BigDecimal].getOrElse(BigDecimal(0))
    val currency = (request.body \ "currency").asOpt[String].getOrElse("USD")
    val reason   = (request.body \ "reason").asOpt[String].getOrElse("unknown")

    logger.warn(
      s"[HIGH-VALUE-PROCESSING-REVERT-SUCCEED] Cancelling processing: $processingId " +
        s"(amount=$amount $currency, reason=$reason, aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    Ok(
      Json.obj(
        "status" -> "cancelled",
        "message" -> "High-value processing cancelled successfully",
        "processingId" -> processingId,
        "refund" -> Json.obj(
          "refundId" -> s"RFD-${processingId}-${System.currentTimeMillis()}",
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

  /** Cancel high-value processing - ALWAYS FAILS (revert endpoint).
    */
  def cancelFail: Action[JsValue] = Action(parse.json) { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType   = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination = request.headers.get("X-Revert-Destination").getOrElse("unknown")

    logger.error(
      s"[HIGH-VALUE-PROCESSING-REVERT-FAIL] Simulating cancellation failure " +
        s"(aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "high_value_cancellation_unavailable",
        "message" -> "High-value processing cancellation service is temporarily unavailable",
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    ).withHeaders("Retry-After" -> "5")
  }
}
