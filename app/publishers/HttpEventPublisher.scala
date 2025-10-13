package publishers

import com.jayway.*
import models.*
import play.api.db.slick.DatabaseConfigProvider
import play.api.libs.json.*
import play.api.libs.ws.DefaultBodyReadables.readableAsString
import play.api.libs.ws.DefaultBodyWritables.*
import play.api.libs.ws.JsonBodyWritables.writeableOf_JsValue
import play.api.libs.ws.{ WSClient, WSRequest, WSResponse }
import play.api.{ Configuration, Logger }
import repositories.{ DestinationResultRepository, OutboxEventsPostgresProfile }
import slick.jdbc.JdbcBackend.Database

import java.net.*
import java.time.Instant
import java.util.concurrent.*
import javax.inject.*
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.*

@Singleton
class HttpEventPublisher @Inject() (
    ws: WSClient,
    config: Configuration,
    eventRouter: EventRouter,
    resultRepo: DestinationResultRepository,
    dbConfigProvider: DatabaseConfigProvider
)(using ec: ExecutionContext)
    extends EventPublisher {

  private val logger  = Logger(this.getClass)
  private val db      = dbConfigProvider.get[OutboxEventsPostgresProfile].db.asInstanceOf[Database]
  private val allowed = Set("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD")

  override def publish(event: OutboxEvent): Future[PublishResult] =
    publishSequentially(event, eventRouter.routeEventToMultiple(event), RoutingContext())

  /** Publishes to specific endpoints (used for revert operations where endpoints are pre-determined).
    */
  def publishToEndpoints(
      event: OutboxEvent,
      endpoints: List[HttpEndpointConfig]
  ): Future[PublishResult] =
    publishSequentially(event, endpoints, RoutingContext())

  /** Publishes to an endpoint and returns both the result and the response payload.
    * This is used for conditional routing to capture responses for condition evaluation.
    */
  private def publishToEndpointWithContext(
      event: OutboxEvent,
      endpoint: HttpEndpointConfig
  ): Future[(PublishResult, Option[JsValue])] =
    endpoint.url match {
      case None =>
        Future.successful(
          (
            PublishResult.NonRetryable(
              s"No URL configured for endpoint: ${endpoint.destinationName}"
            ),
            None
          )
        )

      case Some(_) =>
        val payloadOpt             = event.getPayloadFor(endpoint.destinationName)
        val (request, resolvedUrl) = buildRequest(event, endpoint, payloadOpt)
        val methodUpper            = endpoint.method.toUpperCase

        if (!allowed(methodUpper))
          Future.successful(
            (PublishResult.NonRetryable(s"Unsupported HTTP method: $methodUpper"), None)
          )
        else
          timedEither(httpCall(request, methodUpper, payloadOpt)).map {
            case (Right(resp), duration) =>
              val result       = validateResponse(resp, endpoint)
              val responseJson = scala.util.Try(resp.json).toOption

              saveResult(
                event.aggregateId,
                event.eventType,
                endpoint.withResolvedUrl(resolvedUrl),
                payloadOpt,
                Some(resp.status),
                responseJson,
                duration,
                result == PublishResult.Success,
                None
              )

              (result, responseJson)

            case (Left(ex), duration) =>
              val errorType = ex match {
                case _: ConnectException => "Connection failed"
                case _: TimeoutException => "Timeout"
                case _ => "Error calling"
              }
              val errorMsg = s"$errorType ${endpoint.url}: ${ex.getMessage}"

              saveResult(
                event.aggregateId,
                event.eventType,
                endpoint,
                payloadOpt,
                None,
                None,
                duration,
                success = false,
                Some(errorMsg)
              )

              (PublishResult.Retryable(errorMsg), None)
          }
    }

  private def validateResponse(response: WSResponse, endpoint: HttpEndpointConfig): PublishResult =
    response.status match {
      case status if status >= 200 && status < 300 =>
        PublishResult.Success

      // DELETE + 404 = Success (idempotent: resource already deleted)
      case 404 if endpoint.method.toUpperCase == "DELETE" =>
        PublishResult.Success

      // 429 Too Many Requests - respect Retry-After header
      case 429 =>
        val retryAfter = parseRetryAfter(response, endpoint.retryAfter)
        PublishResult.Retryable(
          s"Rate limited (429): ${response.body[String].take(500)}",
          retryAfter
        )

      // 503 Service Unavailable - often includes Retry-After
      case 503 =>
        val retryAfter = parseRetryAfter(response, endpoint.retryAfter)
        PublishResult.Retryable(
          s"Service unavailable (503): ${response.body[String].take(500)}",
          retryAfter
        )

      case status if status >= 400 && status < 500 =>
        PublishResult.NonRetryable(
          s"HTTP $status: ${response.body[String].take(500)}"
        )

      case status =>
        val retryAfter = parseRetryAfter(response, endpoint.retryAfter)
        PublishResult.Retryable(
          s"HTTP $status: ${response.body[String].take(500)}",
          retryAfter
        )
    }

  /** Parses Retry-After header/body based on configuration.
    */
  private def parseRetryAfter(
      response: WSResponse,
      config: RetryAfterConfig
  ): Option[Instant] = {
    val parsedOpt = if (config.fromBody) {
      parseRetryAfterFromBody(response, config)
    } else {
      parseRetryAfterFromHeader(response, config)
    }

    parsedOpt.orElse(config.fallbackSeconds.map(s => Instant.now().plusSeconds(s)))
  }

  private def parseRetryAfterFromHeader(
      response: WSResponse,
      config: RetryAfterConfig
  ): Option[Instant] =
    response.header(config.headerName).flatMap(value => parseHeaderValue(value, config.format))

  private def parseRetryAfterFromBody(
      response: WSResponse,
      config: RetryAfterConfig
  ): Option[Instant] = for {
    jsonPath <- config.jsonPath
    value <- Try(
      jsonpath.JsonPath.read[Any](response.json.toString, jsonPath).toString
    ).toOption
    instant <- parseHeaderValue(value, config.format)
  } yield instant

  private def parseHeaderValue(value: String, format: RetryAfterFormat): Option[Instant] =
    format match {
      case RetryAfterFormat.Auto =>
        parseAsSeconds(value)
          .orElse(parseAsUnix(value))
          .orElse(parseAsHttpDate(value))
          .orElse(parseAsIso8601(value))
      case RetryAfterFormat.Seconds => parseAsSeconds(value)
      case RetryAfterFormat.Milliseconds => parseAsMilliseconds(value)
      case RetryAfterFormat.Unix => parseAsUnix(value)
      case RetryAfterFormat.HttpDate => parseAsHttpDate(value)
      case RetryAfterFormat.Iso8601 => parseAsIso8601(value)
    }

  private inline def parseAsSeconds(value: String): Option[Instant] =
    Try(value.toLong).toOption.map(seconds => Instant.now().plusSeconds(seconds))

  private inline def parseAsMilliseconds(value: String): Option[Instant] =
    Try(value.toLong).toOption.map(millis => Instant.now().plusMillis(millis))

  private inline def parseAsUnix(value: String): Option[Instant] =
    Try(Instant.ofEpochSecond(value.toLong)).toOption

  private def parseAsHttpDate(value: String): Option[Instant] =
    Try {
      import java.time.ZonedDateTime
      import java.time.format.DateTimeFormatter
      ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant
    }.toOption

  private inline def parseAsIso8601(value: String): Option[Instant] =
    Try(Instant.parse(value)).toOption

  /** Saves aggregate result for an API call.
    */
  private def saveResult(
      aggregateId: String,
      eventType: String,
      endpoint: HttpEndpointConfig,
      requestPayload: Option[JsValue],
      responseStatus: Option[Int],
      responsePayload: Option[JsValue],
      durationMs: Int,
      success: Boolean,
      errorMessage: Option[String]
  ): Unit = {
    val result = AggregateResult(
      aggregateId     = aggregateId,
      eventType       = eventType,
      destination     = endpoint.destinationName,
      endpointUrl     = endpoint.url.getOrElse(""),
      httpMethod      = endpoint.method,
      requestPayload  = requestPayload,
      responseStatus  = responseStatus,
      responsePayload = responsePayload,
      publishedAt     = Instant.now(),
      durationMs      = Some(durationMs),
      success         = success,
      errorMessage    = errorMessage
    )

    db.run(resultRepo.upsert(result)).recover { case ex =>
      logger.warn(
        s"Failed to save aggregate result for $aggregateId to ${endpoint.destinationName}: ${ex.getMessage}"
      )
    }
  }

  /** Replaces placeholders in URL template with actual path parameter values.
    * Example: "/orders/{orderId}" + Map("orderId" -> "123") => "/orders/123"
    */
  private inline def replaceUrlPlaceholders(
      urlTemplate: String,
      pathParams: Map[String, String]
  ): String = pathParams.replacePlaceholders(urlTemplate)

  private def buildRequest(
      event: OutboxEvent,
      endpoint: HttpEndpointConfig,
      payloadOpt: Option[JsValue]
  ): (WSRequest, String) = {
    val pathParams  = event.getPathParamsFor(endpoint.destinationName)
    val resolvedUrl = endpoint.url match {
      case Some(url) => replaceUrlPlaceholders(url, pathParams)
      case None =>
        throw new IllegalStateException(
          s"buildRequest called with endpoint without URL: ${endpoint.destinationName}"
        )
    }

    val baseHeaders = Seq(
      "X-Event-Id" -> event.id.toString,
      "X-Event-Type" -> event.eventType,
      "X-Aggregate-Id" -> event.aggregateId,
      "X-Destination" -> endpoint.destinationName,
      "X-Created-At" -> event.createdAt.toString
    )

    val headers = payloadOpt match {
      case Some(_) => ("Content-Type" -> "application/json") +: baseHeaders
      case None => baseHeaders
    }

    val request = ws
      .url(resolvedUrl)
      .addHttpHeaders(headers: _*)
      .addHttpHeaders(endpoint.headers.toSeq: _*)
      .withRequestTimeout(endpoint.timeout)

    (request, resolvedUrl)
  }

  private def httpCall(
      request: WSRequest,
      methodUpper: String,
      payloadOpt: Option[JsValue]
  ): Future[WSResponse] =
    methodUpper match {
      case "GET" => request.get()
      case "POST" =>
        payloadOpt match {
          case Some(payload) => request.post(payload)
          case None => request.post(Json.obj())
        }
      case "PUT" =>
        payloadOpt match {
          case Some(payload) => request.put(payload)
          case None => request.put(Json.obj())
        }
      case "PATCH" =>
        payloadOpt match {
          case Some(payload) => request.patch(payload)
          case None => request.patch(Json.obj())
        }
      case "DELETE" => request.delete()
      case "HEAD" => request.head()
    }

  /** Capture either success or failure with elapsed ms. */
  private def timedEither[T](
      f: => Future[T],
      start: Long = System.currentTimeMillis()
  ): Future[(Either[Throwable, T], Int)] =
    f.map(Right(_))
      .recover { case e => Left(e) }
      .map(res => (res, (System.currentTimeMillis() - start).toInt))

  /** Publishes to multiple endpoints sequentially with conditional routing support.
    * @param event The event to publish
    * @param endpoints List of endpoints to publish to
    * @param routingContext Tracks responses from previous destinations
    */
  private def publishSequentially(
      event: OutboxEvent,
      endpoints: List[HttpEndpointConfig],
      routingContext: RoutingContext
  ): Future[PublishResult] = endpoints match {
    case Nil =>
      Future.successful(PublishResult.Success)

    case endpoint :: remainingEndpoints =>
      val payloadOpt                = event.getPayloadFor(endpoint.destinationName)
      val needsPayloadForConditions = endpoint.routes.nonEmpty || endpoint.condition.isDefined

      val resolvedEndpoint = (payloadOpt, needsPayloadForConditions) match {
        case (None, true) =>
          // No payload but conditional routing requires it, skip
          logger.warn(
            s"Skipping ${endpoint.destinationName} - conditional routing requires payload but none found"
          )
          None

        case (None, false) =>
          // No payload and no conditions, proceed with call (e.g., DELETE without body)
          logger.debug(
            s"No payload for ${endpoint.destinationName}, but no conditions to evaluate - proceeding"
          )
          Some(endpoint)

        case (Some(requestPayload), _) =>
          if (endpoint.routes.nonEmpty) {
            val matchingRoute = endpoint.routes.find { routeOpt =>
              ConditionEvaluator.evaluate(routeOpt.condition, routingContext, requestPayload)
            }

            matchingRoute match {
              case Some(route) =>
                logger.info(s"Selected route for ${endpoint.destinationName}: ${route.url}")
                Some(
                  endpoint.copy(
                    url    = Some(route.url),
                    revert = route.revert.orElse(endpoint.revert)
                  )
                )
              case None =>
                // No route matched - skip endpoint
                logger.info(s"No matching route for ${endpoint.destinationName}, skipping")
                None
            }
          } else
            endpoint.condition match {
              case None => Some(endpoint) // No condition, always execute
              case Some(condition) =>
                val result = ConditionEvaluator.evaluate(condition, routingContext, requestPayload)
                if (!result) {
                  logger.info(
                    s"Skipping ${endpoint.destinationName} for aggregate ${event.aggregateId} - " +
                      s"condition not met: ${condition.jsonPath} ${condition.operator} ${condition.value.getOrElse("")}"
                  )
                }
                if (result) Some(endpoint) else None
            }
      }

      resolvedEndpoint match {
        case None =>
          // Endpoint skipped (no matching route, condition not met, or missing required payload)
          publishSequentially(event, remainingEndpoints, routingContext)

        case Some(selectedEndpoint) =>
          // Publish to the selected endpoint and handle result
          publishToEndpointWithContext(event, selectedEndpoint).flatMap {
            case (PublishResult.Success, responseOpt) =>
              // Update routing context with response (if available)
              val updatedContext = responseOpt match {
                case Some(response) =>
                  routingContext.withResponse(endpoint.destinationName, response)
                case None => routingContext
              }
              // Current endpoint succeeded, continue to next with updated context
              publishSequentially(event, remainingEndpoints, updatedContext)

            case (retryable: PublishResult.Retryable, _) =>
              // Failed with retryable error, stop and return failure
              Future.successful(retryable)

            case (nonRetryable: PublishResult.NonRetryable, _) =>
              // Failed with non-retryable error, stop and return failure
              Future.successful(nonRetryable)
          }
      }
  }
}
