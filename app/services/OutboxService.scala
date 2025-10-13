package services

import actors.{OutboxProcessor, OutboxProcessorRouter}
import actors.OutboxProcessor.{ProcessEvent, ProcessUnhandledEvent}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.util.Timeout
import play.api.*
import play.api.inject.ApplicationLifecycle
import publishers.EventPublisher
import repositories.OutboxRepository
import slick.jdbc.JdbcBackend.Database

import javax.inject.*
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

@Singleton
class OutboxService @Inject() (
    lifecycle: ApplicationLifecycle,
    db: Database,
    publisher: EventPublisher,
    outboxRepo: OutboxRepository,
    config: Configuration
)(using system: ActorSystem[Nothing], ec: ExecutionContext)
    extends Logging {

  private val pollInterval =
    config.get[FiniteDuration]("outbox.pollInterval")

  private val batchSize =
    config.get[Int]("outbox.batchSize")

  private val poolSize =
    config.getOptional[Int]("outbox.poolSize").getOrElse(1)

  private val outboxActor: ActorRef[OutboxProcessor.Command] =
    if (poolSize > 1) {
      logger.info(s"Starting outbox processor with pool router (size: $poolSize)")
      system.systemActorOf(
        OutboxProcessorRouter(
          db,
          publisher,
          outboxRepo,
          pollInterval,
          batchSize,
          poolSize
        ),
        "outbox-processor-pool"
      )
    } else {
      logger.info("Starting single outbox processor")
      system.systemActorOf(
        OutboxProcessor(db, publisher, outboxRepo, pollInterval, batchSize),
        "outbox-processor"
      )
    }
  logger.info(s"Outbox processor started (poll: $pollInterval, batch: $batchSize)")

  def processEvent(id: Long): Future[Long] =
    outboxActor.ask(ref => ProcessEvent(id, ref))

  outboxActor ! ProcessUnhandledEvent

  lifecycle.addStopHook { () =>
    logger.info("Stopping outbox processor")
    outboxActor
      .ask(replyTo => OutboxProcessor.Stop(replyTo))
      .map { _ =>
        logger.info("Outbox processor stopped gracefully")
      }
      .recover { case ex =>
        logger.error("Error stopping outbox processor", ex)
      }
  }

  def getStats: Future[(Int, Int)] =
    for {
      pending <- db.run(outboxRepo.countPending)
      processed <- db.run(outboxRepo.countProcessed)
    } yield (pending, processed)

  private given Timeout = Timeout(10.seconds)
}
