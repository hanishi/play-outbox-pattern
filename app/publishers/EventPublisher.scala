package publishers

import models.OutboxEvent

import java.time.Instant
import scala.concurrent.Future

trait EventPublisher {
  def publish(event: OutboxEvent): Future[PublishResult]
}

sealed trait PublishResult
object PublishResult {

  /** Event failed but can be retried
    * @param error The error message
    * @param retryAfter When to retry this event (None = use default exponential backoff)
    */
  case class Retryable(
      error: String,
      retryAfter: Option[Instant] = None
  ) extends PublishResult

  /** Event failed and should not be retried (moved to DLQ immediately) */
  case class NonRetryable(error: String) extends PublishResult

  /** Event published successfully */
  case object Success extends PublishResult

}
