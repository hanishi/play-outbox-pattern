package services

import actors.OutboxProcessor
import org.apache.pekko.actor.typed.{ ActorRef, ActorSystem }
import org.apache.pekko.stream.UniqueKillSwitch
import org.apache.pekko.stream.scaladsl.{ Keep, Sink }
import play.api.inject.ApplicationLifecycle
import play.api.{ Configuration, Logging }
import slick.jdbc.JdbcBackend.Database

import javax.inject.*
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

/** Notification service that manages LISTEN/NOTIFY for both outbox and DLQ events.
  * Listens to PostgreSQL notifications and triggers the appropriate processors.
  */
@Singleton
class NotificationService @Inject() (
    lifecycle: ApplicationLifecycle,
    eventProcessing: EventProcessingService,
    config: Configuration
)(using system: ActorSystem[Nothing], ec: ExecutionContext, db: Database)
    extends Logging {

  private val useListenNotify =
    config.getOptional[Boolean]("outbox.useListenNotify").getOrElse(true)

  private val listenKillSwitch: Option[UniqueKillSwitch] =
    if (useListenNotify) {
      logger.info("Starting LISTEN/NOTIFY streams for outbox and DLQ events")

      val streamConfig = PostgresListenNotifyStream.Config(
        channels = Seq("outbox_events_channel", "dlq_events_channel")
      )

      val (killSwitch, doneF) = PostgresListenNotifyStream
        .createChannelSource(streamConfig)
        .map { notification =>
          notification.channel match {
            case "outbox_events_channel" =>
              logger.debug(
                s"Triggering outbox processing for ${notification.eventIds.length} events"
              )
              eventProcessing.outboxActor ! OutboxProcessor.ProcessUnhandledEvent
            case "dlq_events_channel" =>
              logger.debug(
                s"Triggering DLQ processing for ${notification.eventIds.length} events"
              )
              eventProcessing.outboxActor ! OutboxProcessor.TriggerDLQProcessing
            case other =>
              logger.warn(s"Received notification from unknown channel: $other")
          }
        }
        .toMat(Sink.ignore)(Keep.both)
        .run()

      doneF.onComplete {
        case Success(_) =>
          logger.info("LISTEN/NOTIFY streams completed")
        case Failure(ex) =>
          logger.error("LISTEN/NOTIFY streams failed", ex)
      }

      logger.info("LISTEN/NOTIFY streams started for outbox and DLQ events")
      Some(killSwitch)
    } else None

  lifecycle.addStopHook { () =>
    listenKillSwitch.foreach { ks =>
      logger.info("Shutting down LISTEN/NOTIFY streams")
      ks.shutdown()
    }
    scala.concurrent.Future.successful(())
  }
}
