package actors

import actors.OutboxProcessor.Command
import models.OutboxEvent
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import publishers.{ EventPublisher, PublishResult }
import repositories.OutboxRepository
import slick.dbio.DBIO
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.*
import scala.concurrent.duration.*
import scala.util.{ Failure, Success }

object OutboxProcessor {

  def apply(
      db: Database,
      publisher: EventPublisher,
      outboxRepo: OutboxRepository,
      pollInterval: FiniteDuration = 5.seconds,
      batchSize: Int               = 100
  ): Behavior[Command] = Behaviors.setup { context =>
    context.log.info("OutboxProcessor starting")

    Behaviors.withTimers { timers =>
      Behaviors.withStash(100) { stash =>
        timers.startSingleTimer(ProcessUnhandledEvent, 1.second)

        new OutboxProcessor(
          db,
          publisher,
          outboxRepo,
          pollInterval,
          batchSize,
          context,
          stash,
          timers
        ).idle
      }
    }
  }

  sealed trait Command

  case class Stop(replyTo: ActorRef[Stopped.type]) extends Command

  case class ProcessEvent(id: Long, replyTo: ActorRef[Long]) extends Command

  private case class ProcessingComplete(processed: Int, failed: Int) extends Command

  private case class ProcessingFailed(ex: Throwable) extends Command

  case object Stopped

  case object ProcessUnhandledEvent extends Command
}

class OutboxProcessor(
    db: Database,
    publisher: EventPublisher,
    outboxRepo: OutboxRepository,
    pollInterval: FiniteDuration,
    batchSize: Int,
    context: ActorContext[OutboxProcessor.Command],
    stash: StashBuffer[OutboxProcessor.Command],
    timers: TimerScheduler[Command]
) {

  import OutboxProcessor.*

  private implicit val ec: ExecutionContext = context.executionContext
  private val log                           = context.log

  private def idle: Behavior[Command] = Behaviors.receiveMessage {
    case ProcessEvent(id, replyTo) =>
      context.log.debug(s"Processing outbox event: $id")
      context.pipeToSelf(processOutbox(id)) {
        case Success((processed, failed)) => ProcessingComplete(processed, failed)
        case Failure(ex) => ProcessingFailed(ex)
      }
      replyTo ! id
      processing
    case ProcessUnhandledEvent =>
      context.log.debug("Processing unhandled outbox events")
      context.pipeToSelf(processOutboxBatch()) {
        case Success((processed, failed)) => ProcessingComplete(processed, failed)
        case Failure(ex) => ProcessingFailed(ex)
      }
      processing
    case Stop(replyTo) =>
      context.log.info("Stopping")
      timers.cancelAll()
      replyTo ! Stopped
      Behaviors.stopped
    case other =>
      stash.stash(other)
      Behaviors.same
  }

  private def processing: Behavior[Command] = Behaviors.receiveMessage {
    case ProcessingComplete(processed, failed) =>
      if (processed > 0 || failed > 0) {
        context.log.info(s"Processed: $processed, Failed: $failed")
      }
      timers.startSingleTimer(ProcessUnhandledEvent, pollInterval)
      stash.unstashAll(idle)
    case ProcessingFailed(ex) =>
      context.log.error("Processing failed", ex)
      timers.startSingleTimer(ProcessUnhandledEvent, pollInterval)
      stash.unstashAll(idle)
    case Stop(replyTo) =>
      context.log.info("Stopping while processing")
      timers.cancelAll()
      replyTo ! Stopped
      Behaviors.stopped
    case other =>
      stash.stash(other)
      Behaviors.same
  }

  private def processOutboxBatch(): Future[(Int, Int)] = for {
    event <- db.run(outboxRepo.findUnprocessed(batchSize))
    results <- Future.sequence(event.map(processEvent))
  } yield (results.count(_ == true), results.count(_ == false))

  private def processOutbox(id: Long): Future[(Int, Int)] = for {
    event <- db.run(outboxRepo.find(id))
    result <- processEvent(event)
  } yield if (result) (1, 0) else (0, 1)

  private def processEvent(event: OutboxEvent): Future[Boolean] = publisher
    .publish(event)
    .flatMap {
      case PublishResult.Success =>
        log.debug(s"Event ${event.id} published")
        db.run(outboxRepo.markProcessed(event.id)).map(_ => true)
      case PublishResult.Retryable(error) =>
        log.warn(s"Event ${event.id} failed (retryable): $error")
        handleRetryableFailure(event, error)
      case PublishResult.NonRetryable(error) =>
        log.error(s"Event ${event.id} failed (non-retryable): $error")
        handleNonRetryableFailure(event, error)
    }
    .recoverWith { case ex: Throwable =>
      log.error(s"Unexpected error: ${event.id}", ex)
      handleRetryableFailure(event, ex.getMessage)
    }

  private def handleRetryableFailure(event: OutboxEvent, error: String): Future[Boolean] = {

    val action = for {
      _ <- outboxRepo.setError(event.id, error)
      _ <- outboxRepo.incrementRetryCount(event.id, error)
      shouldDLQ = event.retryCount >= 2
      _ <-
        if (shouldDLQ) {
          outboxRepo.moveToDLQ(event, "MAX_RETRIES_EXCEEDED", error)
        } else {
          DBIO.successful(0L)
        }
    } yield {
      if (shouldDLQ) {
        log.warn(s"Event ${event.id} exceeded retries, moving to DLQ")
      }
      false
    }

    db.run(action)
  }

  private def handleNonRetryableFailure(event: OutboxEvent, error: String): Future[Boolean] = {
    val action = for {
      _ <- outboxRepo.setError(event.id, error)
      _ <- outboxRepo.moveToDLQ(event, "NON_RETRYABLE_ERROR", error)
    } yield false

    db.run(action)
  }
}
