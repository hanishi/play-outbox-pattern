package publishers

import models.OutboxEvent

import scala.concurrent.Future

trait EventPublisher {
  def publish(event: OutboxEvent): Future[PublishResult]
}

sealed trait PublishResult
object PublishResult {
  case object Success extends PublishResult
  case class Retryable(error: String) extends PublishResult
  case class NonRetryable(error: String) extends PublishResult
}
