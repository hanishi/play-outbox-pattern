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
    * - 500 Internal Server Error (retryable)
    * - 503 Service Unavailable (retryable)
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

      if (failureType < 0.3) {
        // 30% of failures: 400 Bad Request (non-retryable)
        logger.error(s"[WEBHOOK] Simulating 400 Bad Request for event $eventId")
        BadRequest(
          Json.obj(
            "error" -> "Invalid payload format",
            "eventId" -> eventId,
            "message" -> "This is a simulated non-retryable error"
          )
        )
      } else if (failureType < 0.7) {
        // 40% of failures: 500 Internal Server Error (retryable)
        logger.error(s"[WEBHOOK] Simulating 500 Internal Server Error for event $eventId")
        InternalServerError(
          Json.obj(
            "error" -> "Internal processing error",
            "eventId" -> eventId,
            "message" -> "This is a simulated retryable error"
          )
        )
      } else {
        // 30% of failures: 503 Service Unavailable (retryable)
        logger.error(s"[WEBHOOK] Simulating 503 Service Unavailable for event $eventId")
        ServiceUnavailable(
          Json.obj(
            "error" -> "Service temporarily unavailable",
            "eventId" -> eventId,
            "message" -> "This is a simulated retryable error",
            "retryAfter" -> 5
          )
        )
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
    */
  def alwaysSucceed: Action[JsValue] = Action(parse.json) { request =>
    val eventId = request.headers.get("X-Event-Id").getOrElse("unknown")
    logger.info(s"[WEBHOOK-SUCCESS] Received event $eventId")

    Ok(
      Json.obj(
        "status" -> "success",
        "eventId" -> eventId,
        "message" -> "Always succeeds"
      )
    )
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
}
