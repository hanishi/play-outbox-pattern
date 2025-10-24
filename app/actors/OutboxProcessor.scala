package actors

import actors.OutboxProcessor.Command
import models.OutboxEvent
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import org.apache.pekko.stream.scaladsl.{ Keep, RestartSource, Sink, Source }
import org.apache.pekko.stream.{ KillSwitches, RestartSettings, UniqueKillSwitch }
import org.postgresql.PGConnection
import play.api.Logger
import publishers.{ EventPublisher, PublishResult }
import repositories.OutboxRepository
import slick.jdbc.JdbcBackend.Database

import java.sql.Connection
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.util.control.Exception.*
import scala.util.{ Failure, Success, Try, Using }

object OutboxProcessor {
  private val logger = Logger(this.getClass)

  def apply(
      db: Database,
      publisher: EventPublisher,
      outboxRepo: OutboxRepository,
      pollInterval: FiniteDuration    = 5.seconds,
      batchSize: Int                  = 100,
      maxRetries: Int                 = 2,
      useListenNotify: Boolean        = false,
      staleCleanupEnabled: Boolean    = true,
      staleTimeoutMinutes: Int        = 5,
      cleanupInterval: FiniteDuration = 1.minute
  ): Behavior[Command] = Behaviors.setup { context =>
    logger.info(s"OutboxProcessor.apply() called, useListenNotify=$useListenNotify")
    context.log.info(
      s"OutboxProcessor starting (maxRetries: $maxRetries, useListenNotify: $useListenNotify)"
    )

    // Start PostgreSQL LISTEN/NOTIFY stream if enabled
    val listenKillSwitch: Option[UniqueKillSwitch] =
      if (useListenNotify) {
        logger.info("STARTING LISTEN/NOTIFY SETUP")
        given ActorSystem[Nothing] = context.system
        given ExecutionContext     = context.system.dispatchers.lookup(
          DispatcherSelector.fromConfig("blocking-jdbc")
        )

        def openConnection(): (Connection, PGConnection) = {
          logger.debug("openConnection() called")
          val conn = db.source.createConnection()
          conn.setAutoCommit(true)
          val pg = conn.unwrap(classOf[PGConnection])
          Using.resource(conn.createStatement())(_.execute("LISTEN outbox_events_channel"))
          logger.debug("Connection opened")
          (conn, pg)
        }

        def closeConnection(conn: Connection): Done = {
          Try(Using.resource(conn.createStatement())(_.execute("UNLISTEN *")))
          Try(conn.close())
          Done
        }

        val restartSettings     = RestartSettings(500.millis, 30.seconds, 0.2)
        val (killSwitch, doneF) = RestartSource
          .withBackoff(restartSettings) { () =>
            Source
              .unfoldResourceAsync[Seq[Long], Connection](
                create = () =>
                  Future {
                    catching(classOf[Throwable])
                      .withApply { ex =>
                        logger.error(s"ERROR in openConnection: $ex", ex)
                        throw ex
                      }
                      .apply(openConnection()._1)
                  },
                read = conn =>
                  Future {
                    val pg            = conn.unwrap(classOf[PGConnection])
                    val notifications = Option(pg.getNotifications(200)).toList.flatten
                    val eventIds      = notifications
                      .filter(_.getName == "outbox_events_channel")
                      .flatMap(n => Try(n.getParameter.toLong).toOption)

                    if (eventIds.nonEmpty) {
                      logger.info(
                        s"LISTEN: got ${eventIds.length} event IDs: ${eventIds.mkString(", ")}"
                      )
                    }

                    // Always keep stream alive, emit event IDs when available
                    Some(eventIds)
                  },
                close = conn => Future(closeConnection(conn))
              )
              .filter(_.nonEmpty) // Only emit when we have event IDs
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .map { eventIds =>
            logger.debug(
              s"Triggering processing for ${eventIds.length} events: ${eventIds.take(5).mkString(", ")}..."
            )
            ProcessUnhandledEvent
          }
          .toMat(Sink.foreach(msg => context.self ! msg))(Keep.both)
          .run()

        doneF.onComplete {
          case Success(_) =>
            logger.info("LISTEN stream completed (will restart automatically)")
          case Failure(ex) =>
            logger.error(s"LISTEN stream failed $ex", ex)
        }(context.executionContext)

        logger.info("Started PostgreSQL LISTEN/NOTIFY stream")
        Some(killSwitch)
      } else None

    Behaviors.withTimers { timers =>
      Behaviors.withStash(Int.MaxValue) { stash =>
        // Start initial processing
        timers.startSingleTimer(ProcessUnhandledEvent, 1.second)

        // With LISTEN/NOTIFY, notifications trigger immediate processing
        // Failed events also trigger immediate reprocessing (100ms)

        // Start stale event cleanup timer if enabled
        if (staleCleanupEnabled) {
          timers.startTimerWithFixedDelay(PerformStaleCleanup, cleanupInterval)
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
          listenKillSwitch,
          context,
          stash,
          timers
        ).idle
      }
    }
  }

  sealed trait Command

  final case class Stats(processed: Int, failed: Int, fetched: Int) {
    def +(success: Boolean): Stats =
      if (success) copy(processed = processed + 1) else copy(failed = failed + 1)
  }

  final case class Stop(replyTo: ActorRef[Stopped.type]) extends Command

  private final case class ProcessingComplete(stats: Stats) extends Command

  private final case class ProcessingFailed(ex: Throwable) extends Command

  private final case class CleanupComplete(count: Int) extends Command

  private final case class CleanupFailed(ex: Throwable) extends Command

  object Stats {
    def apply(fetched: Int): Stats = Stats(0, 0, fetched)
  }

  case object Stopped

  case object ProcessUnhandledEvent extends Command

  private case object PerformStaleCleanup extends Command
}

class OutboxProcessor(
    db: Database,
    publisher: EventPublisher,
    outboxRepo: OutboxRepository,
    pollInterval: FiniteDuration,
    batchSize: Int,
    maxRetries: Int,
    useListenNotify: Boolean,
    staleTimeoutMinutes: Int,
    listenKillSwitch: Option[UniqueKillSwitch],
    context: ActorContext[OutboxProcessor.Command],
    stash: StashBuffer[OutboxProcessor.Command],
    timers: TimerScheduler[Command]
) {

  import OutboxProcessor.*

  private val log = context.log
  // handlers for cleanup operations
  private val cleanupHandler: PartialFunction[Command, Behavior[Command]] = {
    case PerformStaleCleanup =>
      context.log.debug("Starting stale event cleanup")
      performStaleCleanup()
      Behaviors.same
    case CleanupComplete(count) =>
      if (count > 0) {
        context.log.info(s"Successfully reset $count stale events to PENDING")
      }
      Behaviors.same
    case CleanupFailed(ex) =>
      context.log.error("Failed to run stale event cleanup", ex)
      Behaviors.same
  }
  // handler for graceful shutdown
  private val stopHandler: PartialFunction[Command, Behavior[Command]] = { case Stop(replyTo) =>
    context.log.info("Stopping")
    timers.cancelAll()
    listenKillSwitch.foreach(_.shutdown())
    replyTo ! Stopped
    Behaviors.stopped
  }

  private given ExecutionContext = context.executionContext

  private def scheduleNext(): Unit = {
    // Only used in polling mode (when useListenNotify is false)
    timers.startSingleTimer(ProcessUnhandledEvent, pollInterval)
  }

  private def pipeToSelf[T](fut: => Future[T])(onSuccess: T => Command): Unit =
    context.pipeToSelf(fut) {
      case Success(v) => onSuccess(v)
      case Failure(ex) => ProcessingFailed(ex)
    }

  private def idle: Behavior[Command] = Behaviors.receiveMessage {
    case ProcessUnhandledEvent =>
      context.log.debug("Received ProcessUnhandledEvent")
      context.log.debug("Processing unhandled outbox events")
      pipeToSelf(processOutboxBatch())(stats => ProcessingComplete(stats))
      stash.unstashAll(processing)
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

      // If we fetched a full batch or had failures in LISTEN/NOTIFY mode,
      // schedule immediate reprocessing to drain the queue
      if (stats.fetched >= batchSize || (stats.failed > 0 && useListenNotify)) {
        context.log.info(
          s"Batch: ${stats.fetched}/${batchSize}, Failed: ${stats.failed}, scheduling immediate reprocessing"
        )
        timers.startSingleTimer(ProcessUnhandledEvent, 100.millis)
      } else if (!useListenNotify) {
        // Polling mode - always schedule next poll
        context.log.debug(s"Queue drained (${stats.fetched}/${batchSize}), scheduling next poll")
        scheduleNext()
      } else {
        // LISTEN/NOTIFY mode with no failures - wait for notifications
        context.log.debug(
          s"Queue drained (${stats.fetched}/${batchSize}), waiting for notifications"
        )
      }

      stash.unstashAll(idle)
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

  private def processOutboxBatch(): Future[Stats] = {
    logger.info(s"Fetching up to $batchSize unprocessed events")
    db.run(outboxRepo.findAndClaimUnprocessed(batchSize))
      .flatMap { events =>
        val fetchedCount = events.length
        logger.info(s"Fetched $fetchedCount events from database")
        if (fetchedCount > 0) {
          logger.info(s"Event IDs: ${events.map(_.id).mkString(", ")}")
        }
        Future.traverse(events)(publishEvent).map { results =>
          results.foldLeft(Stats(fetchedCount))(_ + _)
        }
      }
      .recoverWith { case ex =>
        logger.error("Error fetching events", ex)
        Future.failed(ex)
      }
  }

  private def publishEvent(event: OutboxEvent): Future[Boolean] = publisher
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

    val nextCount = event.retryCount + 1
    val shouldDLQ = nextCount >= maxRetries

    val action = for {
      _ <- outboxRepo.setError(event.id, error)
      _ <-
        if (shouldDLQ) {
          // Don't increment retry count when moving to DLQ
          // moveToDLQ will set status to PROCESSED
          log.warn(s"Event ${event.id} exceeded retries ($maxRetries), moving to DLQ")
          outboxRepo.moveToDLQ(event, "MAX_RETRIES_EXCEEDED", error)
        } else {
          // Increment retry count and set status back to PENDING for retry
          outboxRepo.incrementRetryCount(event.id, error).map(_ => 0L)
        }
    } yield false

    db.run(action)
  }

  private def handleNonRetryableFailure(event: OutboxEvent, error: String): Future[Boolean] = {
    val action = for {
      _ <- outboxRepo.setError(event.id, error)
      _ <- outboxRepo.moveToDLQ(event, "NON_RETRYABLE_ERROR", error)
    } yield false

    db.run(action)
  }

  private def performStaleCleanup(): Unit = {
    val cleanupFuture = for {
      staleCount <- db.run(outboxRepo.countStaleProcessingEvents(staleTimeoutMinutes))
      resetCount <-
        if (staleCount > 0) {
          logger.warn(
            s"Found $staleCount stale PROCESSING events (timeout: $staleTimeoutMinutes minutes), resetting to PENDING"
          )
          db.run(outboxRepo.resetStaleProcessingEvents(staleTimeoutMinutes))
        } else {
          Future.successful(0)
        }
    } yield resetCount

    context.pipeToSelf(cleanupFuture) {
      case Success(count) => CleanupComplete(count)
      case Failure(ex) => CleanupFailed(ex)
    }
  }
}
