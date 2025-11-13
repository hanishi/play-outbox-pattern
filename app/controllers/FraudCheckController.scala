package controllers

import play.api.{ Configuration, Logging }
import play.api.libs.json.*
import play.api.mvc.*

import javax.inject.*
import scala.util.Random

/** Fraud check service controller - returns risk scores for conditional routing.
  *
  * Endpoints:
  * - POST /api/fraud/check - Perform fraud check and return risk score
  */
@Singleton
class FraudCheckController @Inject() (cc: ControllerComponents, config: Configuration)(
    using ec: scala.concurrent.ExecutionContext
) extends AbstractController(cc)
    with Logging {

  private val random                = new Random()
  private val fraudCheckFailureRate = config.get[Double]("service.failure.rates.fraudCheck.check")

  /** Fraud check endpoint - returns risk score.
    * Even aggregate IDs = low risk (< 50)
    * Odd aggregate IDs = high risk (>= 50)
    * Configurable failure rate from application.conf (service.failure.rates.fraudCheck.check)
    */
  def check: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    // Simulate rate limiting with configurable failure rate
    val shouldRateLimit = random.nextDouble() < fraudCheckFailureRate

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
      ).withHeaders("Retry-After" -> retryAfterSeconds.toString)
    } else {
      // Calculate risk score based on aggregate ID (for demo)
      // Even IDs = low risk (< 50), Odd IDs = high risk (>= 50)
      val riskScore = if (aggregateId.toLongOption.exists(_ % 2 == 0)) 25 else 75

      logger.info(
        s"[FRAUD-CHECK] Event $eventId for aggregate $aggregateId - riskScore: $riskScore"
      )

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

  /** Fraud check endpoint - ALWAYS SUCCEEDS (returns low risk).
    */
  def checkSucceed: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")
    val riskScore   = 25 // Always low risk

    logger.info(
      s"[FRAUD-CHECK-SUCCEED] Event $eventId for aggregate $aggregateId - riskScore: $riskScore"
    )

    Ok(
      Json.obj(
        "status" -> "success",
        "riskScore" -> riskScore,
        "riskLevel" -> "low",
        "checks" -> Json.obj(
          "addressVerification" -> "passed",
          "cvvCheck" -> "passed",
          "velocityCheck" -> "passed"
        ),
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }

  /** Fraud check endpoint - ALWAYS FAILS (rate limited).
    */
  def checkFail: Action[JsValue] = Action(parse.json) { request =>
    val eventId           = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId       = request.headers.get("X-Aggregate-Id").getOrElse("unknown")
    val retryAfterSeconds = 10

    logger.error(
      s"[FRAUD-CHECK-FAIL] Simulating rate limit - Event $eventId for aggregate $aggregateId"
    )

    TooManyRequests(
      Json.obj(
        "error" -> "rate_limit_exceeded",
        "message" -> "Fraud check API rate limit exceeded. Please retry after the specified delay.",
        "retryAfter" -> retryAfterSeconds,
        "aggregateId" -> aggregateId
      )
    ).withHeaders("Retry-After" -> retryAfterSeconds.toString)
  }
}
