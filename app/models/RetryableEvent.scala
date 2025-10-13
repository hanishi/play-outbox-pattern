package models

import java.time.Instant

/** Common trait for events that support retry logic with status tracking.
  */
trait RetryableEvent {
  def status: EventStatus
  def statusChangedAt: Option[Instant]
  def nextRetryAt: Option[Instant]

  def isTerminal: Boolean = status == EventStatus.Processed || status == EventStatus.Failed

  def isProcessing: Boolean = status == EventStatus.Processing

  def isPendingAndReady: Boolean = status == EventStatus.Pending && isReadyForRetry

  private def isReadyForRetry: Boolean = nextRetryAt match {
    case None => true // No backoff, ready immediately
    case Some(retryTime) => Instant.now().isAfter(retryTime)
  }
}
