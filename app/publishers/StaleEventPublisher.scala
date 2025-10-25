package publishers

import models.OutboxEvent
import play.api.Logger

import scala.concurrent.duration.*
import scala.concurrent.{ Future, Promise }

/** Publisher that simulates hanging/stale events for testing stale cleanup.
  *
  * Use this to test the stale event recovery mechanism without manually
  * modifying the database.
  */
class StaleEventPublisher extends EventPublisher {

  private val logger = Logger(this.getClass)

  @volatile private var shouldHang                           = false
  @volatile private var hangDuration: Option[FiniteDuration] = None

  /** Enable hanging mode - all publish attempts will hang indefinitely
    */
  def enableHang(): Unit = {
    logger.warn("StaleEventPublisher: Hanging mode ENABLED")
    shouldHang   = true
    hangDuration = None
  }

  /** Enable hanging mode with timeout - publish attempts will hang for specified duration
    */
  def enableHangFor(duration: FiniteDuration): Unit = {
    logger.warn(s"StaleEventPublisher: Hanging mode ENABLED for $duration")
    shouldHang   = true
    hangDuration = Some(duration)
  }

  /** Disable hanging mode - return to normal behavior
    */
  def disableHang(): Unit = {
    logger.info("StaleEventPublisher: Hanging mode DISABLED")
    shouldHang   = false
    hangDuration = None
  }

  override def publish(event: OutboxEvent): Future[PublishResult] =
    if (shouldHang) {
      logger.warn(s"Event ${event.id} will HANG (simulating crashed/stuck worker)")

      hangDuration match {
        case Some(duration) =>
          // Hang for specified duration, then timeout
          logger.warn(s"Event ${event.id} hanging for $duration then timing out")
          val promise = Promise[PublishResult]()
          // Never complete the promise - simulates infinite hang
          // In real scenario, this would be killed by timeout or actor crash
          promise.future

        case None =>
          // Hang indefinitely
          logger.warn(s"Event ${event.id} hanging indefinitely")
          Promise[PublishResult]().future // Never completes
      }
    } else {
      // Normal behavior - succeed
      Future.successful(PublishResult.Success)
    }
}

object StaleEventPublisher {

  /** Create a publisher with hanging mode controlled via JVM property
    *
    * Usage:
    *   -Doutbox.publisher.hang=true
    *   -Doutbox.publisher.hangDuration=30s
    */
  def fromConfig(): StaleEventPublisher = {
    val publisher = new StaleEventPublisher

    // Check for hang mode from system properties
    if (sys.props.get("outbox.publisher.hang").contains("true")) {
      sys.props.get("outbox.publisher.hangDuration") match {
        case Some(durationStr) =>
          val duration = Duration(durationStr).asInstanceOf[FiniteDuration]
          publisher.enableHangFor(duration)
        case None =>
          publisher.enableHang()
      }
    }

    publisher
  }
}
