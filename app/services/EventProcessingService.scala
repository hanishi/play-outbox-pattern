package services

import actors.{ OutboxProcessor, OutboxProcessorRouter }
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.{ ActorRef, ActorSystem }
import org.apache.pekko.util.Timeout
import play.api.*
import play.api.inject.ApplicationLifecycle
import publishers.{ EventPublisher, EventRouter }
import repositories.{ DeadLetterRepository, DestinationResultRepository, OutboxRepository }
import slick.jdbc.JdbcBackend.Database

import javax.inject.*
import scala.concurrent.duration.*
import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class EventProcessingService @Inject() (
    lifecycle: ApplicationLifecycle,
    publisher: EventPublisher,
    outboxRepo: OutboxRepository,
    dlqRepo: DeadLetterRepository,
    resultRepo: DestinationResultRepository,
    eventRouter: EventRouter,
    config: Configuration
)(using system: ActorSystem[Nothing], ec: ExecutionContext, db: Database)
    extends Logging {

  private val pollInterval =
    Option(config.get[FiniteDuration]("outbox.pollInterval")).filter(_ != null).getOrElse(5.seconds)
  private val batchSize =
    config.get[Int]("outbox.batchSize")
  private val poolSize =
    config.getOptional[Int]("outbox.poolSize").getOrElse(1)
  private val maxRetries =
    config.getOptional[Int]("outbox.maxRetries").getOrElse(2)
  private val useListenNotify =
    config.getOptional[Boolean]("outbox.useListenNotify").getOrElse(true)
  private val staleCleanupEnabled =
    config.getOptional[Boolean]("outbox.enableStaleEventCleanup").getOrElse(true)
  private val staleTimeoutMinutes =
    config.getOptional[Int]("outbox.staleEventTimeoutMinutes").getOrElse(5)
  private val cleanupInterval =
    Option(config.getOptional[FiniteDuration]("outbox.cleanupInterval")).flatten
      .filter(_ != null)
      .getOrElse(1.minute)

  val outboxActor: ActorRef[OutboxProcessor.Command] =
    if (poolSize > 1) {
      logger.info(s"Starting outbox processor with pool router (size: $poolSize)")
      system.systemActorOf(
        OutboxProcessorRouter(
          publisher,
          outboxRepo,
          dlqRepo,
          resultRepo,
          eventRouter,
          pollInterval,
          batchSize,
          poolSize,
          maxRetries,
          useListenNotify,
          staleCleanupEnabled,
          staleTimeoutMinutes,
          cleanupInterval
        ),
        "outbox-processor-pool"
      )
    } else {
      logger.info("Starting single outbox processor")
      system.systemActorOf(
        OutboxProcessor(
          publisher,
          outboxRepo,
          dlqRepo,
          resultRepo,
          eventRouter,
          pollInterval,
          batchSize,
          maxRetries,
          useListenNotify,
          staleCleanupEnabled,
          staleTimeoutMinutes,
          cleanupInterval
        ),
        "outbox-processor"
      )
    }

  logger.info(
    s"Event processing started (pool: $poolSize, batch: $batchSize, useListenNotify: $useListenNotify)"
  )

  outboxActor ! OutboxProcessor.ProcessUnhandledEvent

  lifecycle.addStopHook { () =>
    logger.info("Stopping event processors")

    outboxActor
      .ask(replyTo => OutboxProcessor.Stop(replyTo))
      .map(_ => logger.info("Outbox processor (and child DLQ processor) stopped"))
      .recover { case ex => logger.error("Error stopping outbox processor", ex) }
  }

  def getOutboxStats: Future[(Int, Int)] =
    for {
      pending <- db.run(outboxRepo.countPending)
      processed <- db.run(outboxRepo.countProcessed)
    } yield (pending, processed)

  def getDLQStats: Future[(Int, Int, Int)] =
    for {
      pending <- db.run(dlqRepo.countPending)
      processed <- db.run(dlqRepo.countProcessed)
      failed <- db.run(dlqRepo.countFailed)
    } yield (pending, processed, failed)

  private given Timeout = Timeout(10.seconds)
}
