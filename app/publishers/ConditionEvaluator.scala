package publishers

import com.jayway.*
import models.RoutingContext
import play.api.Logger
import play.api.libs.json.JsValue

import scala.util.Try

/** Evaluates conditions for conditional routing based on routing context. */
object ConditionEvaluator {
  private val logger = Logger(this.getClass)

  /** Evaluates a condition against the routing context or request payload.
    * @param condition The condition to evaluate
    * @param routingContext The routing context with previous responses
    * @param requestPayload The original request payload (used when no previousDestination specified)
    * @return true if condition is met, false otherwise
    */
  def evaluate(
      condition: Condition,
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
    .exists(json => evaluateCondition(condition, json))

  private def evaluateCondition(condition: Condition, json: JsValue): Boolean =
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

  private def compareNumeric(
      extractedStr: String,
      valueOpt: Option[String]
  )(cmp: (Double, Double) => Boolean): Boolean = (for {
    expectedStr <- valueOpt
    extractedNum <- extractedStr.toDoubleOption
    expectedNum <- expectedStr.toDoubleOption
  } yield cmp(extractedNum, expectedNum)).getOrElse(false)
}
