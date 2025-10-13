package publishers

import models.OutboxEvent
import play.api.Configuration
import play.api.libs.ws.DefaultBodyReadables.readableAsString
import play.api.libs.ws.DefaultBodyWritables.*
import play.api.libs.ws.{ WSClient, WSResponse }

import javax.inject.*
import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class HttpEventPublisher @Inject() (
    ws: WSClient,
    config: Configuration,
    eventRouter: EventRouter
)(using ec: ExecutionContext)
    extends EventPublisher {

  override def publish(event: OutboxEvent): Future[PublishResult] = {
    val endpoint = eventRouter.routeEvent(event)

    val request = ws
      .url(endpoint.url)
      .addHttpHeaders(
        "Content-Type" -> "application/json",
        "X-Event-Id" -> event.id.toString,
        "X-Event-Type" -> event.eventType,
        "X-Aggregate-Id" -> event.aggregateId,
        "X-Created-At" -> event.createdAt.toString
      )
      .addHttpHeaders(endpoint.headers.toSeq: _*)
      .withRequestTimeout(endpoint.timeout)

    val response = endpoint.method.toUpperCase match {
      case "GET" => request.get()
      case "POST" => request.post(event.payload)
      case "PUT" => request.put(event.payload)
      case "PATCH" => request.patch(event.payload)
      case "DELETE" => request.delete()
      case "HEAD" => request.head()
      case other =>
        return Future.successful(
          PublishResult.NonRetryable(s"Unsupported HTTP method: $other")
        )
    }

    response
      .map(resp => validateResponse(resp, endpoint.method))
      .recover {
        case ex: java.net.ConnectException =>
          PublishResult.Retryable(s"Connection failed: ${ex.getMessage}")
        case ex: java.util.concurrent.TimeoutException =>
          PublishResult.Retryable(s"Timeout: ${ex.getMessage}")
        case ex =>
          PublishResult.Retryable(s"Error: ${ex.getMessage}")
      }
  }

  private def validateResponse(response: WSResponse, method: String): PublishResult =
    response.status match {
      case status if status >= 200 && status < 300 =>
        PublishResult.Success

      // DELETE + 404 = Success (idempotent: resource already deleted)
      case 404 if method.toUpperCase == "DELETE" =>
        PublishResult.Success

      case status if status >= 400 && status < 500 =>
        PublishResult.NonRetryable(
          s"HTTP $status: ${response.body[String].take(500)}"
        )

      case status =>
        PublishResult.Retryable(
          s"HTTP $status: ${response.body[String].take(500)}"
        )
    }
}
