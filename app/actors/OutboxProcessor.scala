package actors

import models.OutboxEvent
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import play.api.Logger
import publishers.{ EventPublisher, HttpEventPublisher, PublishResult }
import repositories.OutboxRepository
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api.DBIO

import java.*
import java.time.*
import scala.concurrent.*
import scala.concurrent.duration.*

object OutboxProcessor {
  private val logger = Logger(this.getClass)

  def apply(
      publisher: EventPublisher,
      outboxRepo: OutboxRepository,
      dlqRepo: repositories.DeadLetterRepository,
      resultRepo: repositories.DestinationResultRepository,
      eventRouter: publishers.EventRouter,
      pollInterval: FiniteDuration    = 5.seconds,
      batchSize: Int                  = 100,
      maxRetries: Int                 = 3,
      useListenNotify: Boolean        = false,
      staleCleanupEnabled: Boolean    = true,
      staleTimeoutMinutes: Int        = 5,
      cleanupInterval: FiniteDuration = 1.minute
  )(using db: Database): Behavior[Command] = Behaviors.setup { context =>
    context.log.info(
      s"OutboxProcessor starting (maxRetries: $maxRetries, useListenNotify: $useListenNotify)"
    )

    val dlqProcessor = publisher match {
      case httpEventPublisher: HttpEventPublisher =>
        val processor = context.spawn(
          DLQProcessor(
            httpEventPublisher,
            dlqRepo,
            resultRepo,
            eventRouter,
            pollInterval,
            batchSize,
            maxRetries,
            useListenNotify
          ),
          "dlq-processor"
        )
        context.log.info(s"Started DLQ processor as child actor (maxRetries: $maxRetries)")
        Some(processor)
      case _ =>
        None
    }

    Behaviors.withTimers { timers =>
      Behaviors.withStash(Int.MaxValue) { stash =>

        timers.startSingleTimer(ProcessUnhandledEvent, 1.second)

        if (staleCleanupEnabled) {
          context.self ! PerformStaleCleanup
          timers.startSingleTimer(PerformStaleCleanup, cleanupInterval)
          context.log.info(
            s"Stale event cleanup enabled (timeout: $staleTimeoutMinutes minutes, interval: $cleanupInterval)"
          )
        }

        new OutboxProcessor(
          db,
          publisher,
          outboxRepo,
          pollInterval,
          batchSize,
          maxRetries,
          useListenNotify,
          staleTimeoutMinutes,
          dlqProcessor,
          context,
          stash,
          timers
        ).idle
      }
    }
  }

  sealed trait Command

  final case class Stop(replyTo: ActorRef[Stopped.type]) extends Command

  private final case class ProcessingComplete(stats: EventProcessor.Stats) extends Command

  private final case class ProcessingFailed(ex: Throwable) extends Command

  private final case class CleanupComplete(count: Int) extends Command

  private final case class CleanupFailed(ex: Throwable) extends Command

  private final case class CheckNextDue(nextDue: Option[Instant]) extends Command

  private final case class CheckNextDueFailed(ex: Throwable) extends Command

  case object Stopped

  case object ProcessUnhandledEvent extends Command

  case object TriggerDLQProcessing extends Command

  private case object PerformStaleCleanup extends Command
}

class OutboxProcessor(
    db: Database,
    publisher: EventPublisher,
    outboxRepo: OutboxRepository,
    protected val pollInterval: FiniteDuration,
    batchSize: Int,
    maxRetries: Int,
    useListenNotify: Boolean,
    staleTimeoutMinutes: Int,
    dlqProcessor: Option[ActorRef[DLQProcessor.Command]],
    protected val context: ActorContext[OutboxProcessor.Command],
    protected val stash: StashBuffer[OutboxProcessor.Command],
    protected val timers: TimerScheduler[OutboxProcessor.Command]
) extends EventProcessor.Base[OutboxProcessor.Command] {

  import OutboxProcessor.*

  private val log                                                         = context.log
  private val cleanupHandler: PartialFunction[Command, Behavior[Command]] = {
    case PerformStaleCleanup =>
      context.log.debug("Starting stale event cleanup")
      performStaleCleanup()
      Behaviors.same
    case CleanupComplete(count) =>
      if (count > 0) {
        context.log.info(s"Successfully reset $count stale events to PENDING")
        context.self ! ProcessUnhandledEvent
      }
      Behaviors.same
    case CleanupFailed(ex) =>
      context.log.error("Failed to run stale event cleanup", ex)
      Behaviors.same
  }
  private val stopHandler: PartialFunction[Command, Behavior[Command]] = { case Stop(replyTo) =>

    timers.cancelAll()
    dlqProcessor.foreach { actorRef =>
      context.log.info("Stopping OutboxProcessor and child DLQ processor")
      context.stop(actorRef)
    }
    replyTo ! Stopped
    Behaviors.stopped
  }

  /** Moves an event to DLQ with automatic revert processing.
    *
    * This method is called when an event fails after max retries or encounters
    * a non-retryable error. DLQProcessor will pick up the DLQ event
    * for compensating transaction execution.
    */
  private def moveToDLQWithRevert(
      event: OutboxEvent,
      reason: String,
      error: String
  ): Future[Long] = {
    log.info(
      s"Moving event ${event.id} (aggregate: ${event.aggregateId}) to DLQ with reason: $reason"
    )
    db.run(outboxRepo.moveToDLQ(event, reason, error)).map { dlqId =>
      dlqProcessor.foreach(actorRef => actorRef ! DLQProcessor.ProcessPendingDLQ)
      dlqId
    }
  }

  private def scheduleNext(): Unit = scheduleNext(ProcessUnhandledEvent)

  private def idle: Behavior[Command] = Behaviors.receiveMessage {
    case ProcessUnhandledEvent =>
      context.log.debug("Processing unhandled outbox events")
      pipeToSelf(processOutboxBatch)(stats => ProcessingComplete(stats), e => ProcessingFailed(e))
      stash.unstashAll(processing)
    case TriggerDLQProcessing =>
      dlqProcessor.foreach(_ ! DLQProcessor.ProcessPendingDLQ)
      Behaviors.same
    case other =>
      cleanupHandler
        .orElse(stopHandler)
        .applyOrElse(
          other,
          command => {
            stash.stash(command)
            Behaviors.same
          }
        )
  }

  private def processing: Behavior[Command] = Behaviors.receiveMessage {
    case ProcessingComplete(stats) =>
      if (stats.processed > 0 || stats.failed > 0) {
        context.log.info(
          s"Batch complete - Fetched: ${stats.fetched}, Processed: ${stats.processed}, Failed: ${stats.failed}"
        )
      }
      pipeToSelf(db.run(outboxRepo.getNextDueTime))(
        nextDue => CheckNextDue(nextDue),
        ex => CheckNextDueFailed(ex)
      )
      stash.unstashAll(checkingNextDue)

    case ProcessingFailed(ex) =>
      context.log.error("Processing failed", ex)
      scheduleNext()
      stash.unstashAll(idle)
    case other =>
      cleanupHandler
        .orElse(stopHandler)
        .applyOrElse(
          other,
          command => {
            stash.stash(command)
            Behaviors.same
          }
        )
  }

  private def processOutboxBatch: Future[EventProcessor.Stats] =
    processBatch(
      fetchBatch    = db.run(outboxRepo.findAndClaimUnprocessed(batchSize)),
      processEvent  = publishEvent,
      eventTypeName = "outbox",
      getEventId    = _.id
    )

  private def publishEvent(event: OutboxEvent): Future[Boolean] = publisher
    .publish(event)
    .flatMap {
      case PublishResult.Success =>
        log.debug(s"Event ${event.id} published")
        db.run(outboxRepo.markProcessed(event.id)).map(_ => true)
      case PublishResult.Retryable(error, retryAfter) =>
        log.warn(s"Event ${event.id} failed (retryable): $error, retry after: $retryAfter")
        handleRetryableFailure(event, error, retryAfter)
      case PublishResult.NonRetryable(error) =>
        log.error(s"Event ${event.id} failed (non-retryable): $error")
        handleNonRetryableFailure(event, error)
    }
    .recoverWith { case ex: Throwable =>
      log.error(s"Unexpected error: ${event.id}", ex)
      handleRetryableFailure(event, ex.getMessage, None)
    }

  private def handleRetryableFailure(
      event: OutboxEvent,
      error: String,
      retryAfter: Option[Instant]
  ): Future[Boolean] = {
    val nextCount = event.retryCount + 1
    val shouldDLQ = nextCount > maxRetries

    if (shouldDLQ) {
      log.warn(
        s"Event ${event.id} exceeded retries (retry count: ${event.retryCount}, max: $maxRetries), moving to DLQ"
      )
      val updatedEvent = event.copy(retryCount = nextCount)
      db.run(outboxRepo.setError(event.id, error)).flatMap { _ =>
        moveToDLQWithRevert(updatedEvent, "MAX_RETRIES_EXCEEDED", error).map(_ => false)
      }
    } else {
      val nextRetryAt = retryAfter.getOrElse {
        val backoffSeconds = Math.pow(2, nextCount).toLong
        Instant.now().plusSeconds(backoffSeconds)
      }

      log.info(
        s"Event ${event.id} will retry at $nextRetryAt (attempt $nextCount/$maxRetries)"
      )

      val action = for {
        _ <- outboxRepo.setError(event.id, error)
        _ <- outboxRepo.incrementRetryCount(event.id, event.retryCount, error, nextRetryAt)
      } yield false

      db.run(action)
    }
  }

  private def handleNonRetryableFailure(event: OutboxEvent, error: String): Future[Boolean] =
    db.run(outboxRepo.setError(event.id, error)).flatMap { _ =>
      moveToDLQWithRevert(event, "NON_RETRYABLE_ERROR", error).map(_ => false)
    }

  private def checkingNextDue: Behavior[Command] = Behaviors.receiveMessage {
    case CheckNextDue(nextDue) =>
      nextDue match {
        case None =>
          context.log.debug("No more pending events")
          if (!useListenNotify) {
            scheduleNext()
          }

        case Some(dueTime) if dueTime.isBefore(Instant.now()) =>
          context.log.info("More events ready now, scheduling immediate reprocessing")
          timers.startSingleTimer(ProcessUnhandledEvent, 100.millis)

        case Some(dueTime) =>
          val delayMillis = dueTime.toEpochMilli - Instant.now().toEpochMilli
          context.log.info(
            s"Next event ready in ${delayMillis}ms, scheduling check at exact time"
          )
          timers.startSingleTimer(ProcessUnhandledEvent, delayMillis.millis)
      }
      stash.unstashAll(idle)

    case CheckNextDueFailed(ex) =>
      context.log.error("Failed to check next due time", ex)
      scheduleNext()
      stash.unstashAll(idle)

    case other =>
      cleanupHandler
        .orElse(stopHandler)
        .applyOrElse(
          other,
          command => {
            stash.stash(command)
            Behaviors.same
          }
        )
  }

  private def performStaleCleanup(): Unit = {
    val action = for {
      staleCount <- outboxRepo.countStaleProcessingEvents(staleTimeoutMinutes)
      resetCount <-
        if (staleCount > 0) {
          logger.warn(
            s"Found $staleCount stale PROCESSING events (timeout: $staleTimeoutMinutes minutes), resetting to PENDING"
          )
          outboxRepo.resetStaleProcessingEvents(staleTimeoutMinutes)
        } else {
          DBIO.successful(0)
        }
    } yield resetCount
    pipeToSelf(db.run(action))(count => CleanupComplete(count), e => CleanupFailed(e))
  }

  override protected def logger: Logger = Logger(this.getClass)
}
