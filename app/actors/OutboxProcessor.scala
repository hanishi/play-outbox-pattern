package actors

import models.OutboxEvent
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import play.api.Logger
import publishers.{ EventPublisher, HttpEventPublisher, PublishResult }
import repositories.OutboxRepository
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api.DBIO

import java.time.*
import scala.concurrent.*
import scala.concurrent.duration.*

/** Outbox processor that publishes events from the outbox table to external APIs.
  *
  * == Purpose ==
  * Main entry point for event processing. Polls the outbox_events table, publishes events
  * to external APIs via HttpEventPublisher, and handles retries with exponential backoff.
  * Automatically spawns a child DLQProcessor for Saga compensation when events fail.
  *
  * == Event Flow ==
  *
  * '''Forward Events (e.g., OrderCreated):'''
  * {{{
  * # Step 1: Claim PENDING events from outbox_events
  * SELECT * FROM outbox_events
  * WHERE status = 'PENDING'
  *   AND (next_retry_at IS NULL OR next_retry_at <= NOW())
  * ORDER BY created_at
  * LIMIT 100
  * FOR UPDATE SKIP LOCKED;  -- Concurrency-safe claiming
  *
  * UPDATE outbox_events SET status = 'PROCESSING' WHERE id IN (...);
  *
  * # Step 2: Publish to external APIs (fan-out)
  * # OrderCreated fanout: ["inventory", "fraudCheck", "shipping", "billing"]
  * POST http://localhost:9000/api/inventory/reserve     → Success (reservationId: RES-456)
  * POST http://localhost:9000/api/fraud/check           → Success (riskScore: 25)
  * POST http://localhost:9000/api/domestic-shipping     → Success (shipmentId: SHIP-789)
  * POST http://localhost:9000/api/billing               → FAIL (HTTP 503 Service Unavailable)
  *
  * # Step 3: Handle failure
  * # Attempt 1: Retryable failure → increment retry_count, schedule retry
  * UPDATE outbox_events
  * SET retry_count = 1, next_retry_at = NOW() + INTERVAL '2 seconds', last_error = 'HTTP 503'
  * WHERE id = 123;
  *
  * # Attempt 2 (after 2s): Still failing
  * UPDATE outbox_events
  * SET retry_count = 2, next_retry_at = NOW() + INTERVAL '4 seconds', last_error = 'HTTP 503'
  * WHERE id = 123;
  *
  * # Attempt 3 (after 4s): Still failing
  * UPDATE outbox_events
  * SET retry_count = 3, next_retry_at = NOW() + INTERVAL '8 seconds', last_error = 'HTTP 503'
  * WHERE id = 123;
  *
  * # Step 4: After max retries (3) → Move to DLQ
  * INSERT INTO dead_letter_events (
  *   original_event_id = 123,
  *   aggregate_id = '456',
  *   event_type = 'OrderCreated',
  *   status = 'PENDING',
  *   reason = 'MAX_RETRIES_EXCEEDED',
  *   error = 'HTTP 503 Service Unavailable'
  * );
  * UPDATE outbox_events SET status = 'PROCESSED', moved_to_dlq = true WHERE id = 123;
  *
  * # Step 5: Trigger DLQProcessor for automatic Saga compensation
  * # DLQProcessor reverts in LIFO order: shipping → inventory (fraudCheck skipped)
  * }}}
  *
  * '''Revert Events (e.g., !OrderCreated - manual user cancellation):'''
  * {{{
  * # Step 1: User triggers manual cancellation
  * db.run(orderRepo.cancelWithEvent(orderId = 456, reason = "Customer requested"))
  * # → INSERT INTO outbox_events (event_type='!OrderCreated', aggregate_id='456', status='PENDING')
  *
  * # Step 2: OutboxProcessor claims revert event
  * # Query aggregate_results for successful forward calls:
  * SELECT * FROM aggregate_results
  * WHERE aggregate_id = '456'
  *   AND event_type = 'OrderCreated'
  *   AND success = true
  * ORDER BY fanout_order DESC;  -- LIFO order
  * # Returns: [shipping(2), fraudCheck(1), inventory(0)]
  *
  * # Step 3: Publish revert APIs (LIFO order)
  * POST http://localhost:9000/api/domestic-shipping/SHIP-789/cancel  → Success
  * # fraudCheck skipped (read-only, no revert config)
  * DELETE http://localhost:9000/api/inventory/RES-456/release        → FAIL (HTTP 500)
  *
  * # Step 4: Retryable failure → retry with exponential backoff
  * UPDATE outbox_events
  * SET retry_count = 1, next_retry_at = NOW() + INTERVAL '2 seconds'
  * WHERE id = 789;
  *
  * # After 3 retries still failing → Move to DLQ (tracking only, NOT processed by DLQProcessor)
  * # Reason: Revert events (starting with !) are already compensation - no further auto-compensation
  * INSERT INTO dead_letter_events (
  *   original_event_id = 789,
  *   aggregate_id = '456',
  *   event_type = '!OrderCreated',
  *   status = 'PENDING',  -- But DLQProcessor will NOT pick this up (event_type NOT LIKE '!%')
  *   reason = 'MAX_RETRIES_EXCEEDED'
  * );
  * # Manual intervention required to retry inventory.release
  * }}}
  *
  * == Key Features ==
  *
  * '''Exponential Backoff:'''
  * {{{
  * # Retry delays: 2^n seconds (configurable maxRetries)
  * # Attempt 0: Immediate (0s delay)
  * # Attempt 1: After 2 seconds  (2^1)
  * # Attempt 2: After 4 seconds  (2^2)
  * # Attempt 3: After 8 seconds  (2^3)
  * # After 3 retries → Move to DLQ
  *
  * # Rate Limit Handling (Retry-After header):
  * # API response: HTTP 429 Too Many Requests
  * #               Retry-After: 60
  * # → next_retry_at = NOW() + 60 seconds (respects API rate limit)
  * }}}
  *
  * '''Stale Event Cleanup:'''
  * {{{
  * # Problem: Worker crashes while processing event
  * # Event stuck in PROCESSING state forever
  *
  * # Solution: Automatic cleanup (runs every cleanupInterval)
  * SELECT id FROM outbox_events
  * WHERE status = 'PROCESSING'
  *   AND status_changed_at < NOW() - INTERVAL '5 minutes';  -- staleTimeoutMinutes
  *
  * UPDATE outbox_events SET status = 'PENDING' WHERE id IN (...);
  * # Next poll cycle picks up the event and retries
  * }}}
  *
  * '''Concurrency Control (FOR UPDATE SKIP LOCKED):'''
  * {{{
  * # Scenario: 3 workers processing concurrently
  *
  * # T1: Worker A claims events [1, 2, 3]
  * SELECT * FROM outbox_events ... FOR UPDATE SKIP LOCKED;  -- Locks [1,2,3]
  * UPDATE ... SET status='PROCESSING' WHERE id IN (1,2,3);
  *
  * # T2: Worker B claims events [4, 5, 6]
  * SELECT * FROM outbox_events ... FOR UPDATE SKIP LOCKED;  -- Skips [1,2,3], locks [4,5,6]
  * UPDATE ... SET status='PROCESSING' WHERE id IN (4,5,6);
  *
  * # No overlap, no duplicate processing
  * }}}
  *
  * '''DLQ Behavior:'''
  * {{{
  * # Forward events (OrderCreated) → DLQ triggers automatic compensation
  * moveToDLQ(event, reason, error, triggerProcessor = true)
  * # → DLQProcessor picks up and performs LIFO compensation
  *
  * # Revert events (!OrderCreated) → DLQ for tracking only
  * moveToDLQ(event, reason, error, triggerProcessor = false)
  * # → NOT processed by DLQProcessor (prevents "reverting the revert")
  * # → Requires manual intervention
  *
  * # DLQProcessor filter:
  * SELECT * FROM dead_letter_events
  * WHERE status = 'PENDING'
  *   AND event_type NOT LIKE '!%';  -- Excludes revert events
  * }}}
  *
  * == Complete Example: OrderCreated with Billing Failure ==
  *
  * {{{
  * # 1. User creates order
  * POST /api/orders
  * Body: {"customerId": "C-123", "totalAmount": 99.99, "shippingType": "domestic"}
  *
  * # 2. OrderRepository.createWithEvent inserts into outbox
  * BEGIN
  *   INSERT INTO orders (...) RETURNING id;  -- Returns orderId: 456
  *   INSERT INTO outbox_events (
  *     aggregate_id = '456',
  *     event_type = 'OrderCreated',
  *     payloads = '{"inventory": {...}, "fraudCheck": {...}, "shipping": {...}, "billing": {...}}',
  *     status = 'PENDING'
  *   ) RETURNING id;  -- Returns outbox event id: 123
  * COMMIT
  *
  * # 3. OutboxProcessor claims event 123
  * SELECT * FROM outbox_events WHERE status='PENDING' ... FOR UPDATE SKIP LOCKED;
  * UPDATE outbox_events SET status='PROCESSING' WHERE id=123;
  *
  * # 4. Publish to fanout destinations
  * POST /api/inventory/reserve     → 200 OK (reservationId: RES-456)
  * POST /api/fraud/check           → 200 OK (riskScore: 25)
  * POST /api/domestic-shipping     → 200 OK (shipmentId: SHIP-789)
  * POST /api/billing               → 503 Service Unavailable (billing service down)
  *
  * # 5. Handle billing failure (retryable)
  * UPDATE outbox_events SET retry_count=1, next_retry_at=NOW()+2s, last_error='HTTP 503' WHERE id=123;
  * UPDATE outbox_events SET status='PENDING' WHERE id=123;  -- Reset for retry
  *
  * # 6. Retry attempts (2s, 4s, 8s delays)
  * # All fail with HTTP 503
  *
  * # 7. After 3 retries → Move to DLQ
  * INSERT INTO dead_letter_events (
  *   original_event_id=123, aggregate_id='456', event_type='OrderCreated',
  *   status='PENDING', reason='MAX_RETRIES_EXCEEDED', error='HTTP 503'
  * );
  * UPDATE outbox_events SET status='PROCESSED', moved_to_dlq=true WHERE id=123;
  *
  * # 8. Trigger DLQProcessor (automatic Saga compensation)
  * dlqProcessor ! ProcessPendingDLQ
  *
  * # 9. DLQProcessor queries successful results
  * SELECT * FROM aggregate_results
  * WHERE aggregate_id='456' AND event_type='OrderCreated' AND success=true
  * ORDER BY fanout_order DESC;
  * # Returns: [shipping(2), fraudCheck(1), inventory(0)]  -- LIFO order
  *
  * # 10. DLQProcessor performs compensation (LIFO)
  * POST /api/domestic-shipping/SHIP-789/cancel  → 200 OK (shipment cancelled)
  * # Skip fraudCheck (no revert config, read-only)
  * DELETE /api/inventory/RES-456/release        → 200 OK (inventory released)
  *
  * # 11. Mark DLQ event as PROCESSED
  * UPDATE dead_letter_events SET status='PROCESSED' WHERE id=...;
  *
  * # Result: Order failed, but all successful operations have been compensated
  * # Database state: Order exists with status=CANCELLED (if cancelled by user)
  * # External services: Inventory released, shipment cancelled
  * }}}
  */
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

  /** Moves an event to DLQ, optionally triggering DLQProcessor for automatic compensation.
    *
    * @param event The event to move to DLQ
    * @param reason Reason for DLQ (e.g., MAX_RETRIES_EXCEEDED, NON_RETRYABLE_ERROR)
    * @param error Error message
    * @param triggerProcessor If true, triggers DLQProcessor for compensation. Set to false for revert events.
    */
  private def moveToDLQ(
      event: OutboxEvent,
      reason: String,
      error: String,
      triggerProcessor: Boolean
  ): Future[Long] = {
    val eventTypeLabel = if (event.eventType.startsWith("!")) "Revert event" else "Event"
    log.info(
      s"Moving $eventTypeLabel ${event.id} (${event.eventType}, aggregate: ${event.aggregateId}) " +
        s"to DLQ with reason: $reason (triggerProcessor: $triggerProcessor)"
    )
    db.run(outboxRepo.moveToDLQ(event, reason, error)).map { dlqId =>
      if (triggerProcessor) {
        dlqProcessor.foreach(actorRef => actorRef ! DLQProcessor.ProcessPendingDLQ)
      }
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
          s"Batch complete - Fetched: ${stats.fetched}, ✓ Processed: ${stats.processed}, ❌ Failed: ${stats.failed}"
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
      val isRevertEvent = event.eventType.startsWith("!")
      val eventLabel    = if (isRevertEvent) "Revert event" else "Event"

      log.warn(
        s"$eventLabel ${event.id} (${event.eventType}) exceeded retries " +
          s"(retry count: ${event.retryCount}, max: $maxRetries), moving to DLQ"
      )

      val updatedEvent = event.copy(retryCount = nextCount)
      db.run(outboxRepo.setError(event.id, error)).flatMap { _ =>
        moveToDLQ(updatedEvent, "MAX_RETRIES_EXCEEDED", error, triggerProcessor = !isRevertEvent)
          .map(_ => false)
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

  private def handleNonRetryableFailure(event: OutboxEvent, error: String): Future[Boolean] = {
    val isRevertEvent = event.eventType.startsWith("!")
    val eventLabel    = if (isRevertEvent) "Revert event" else "Event"

    log.error(
      s"$eventLabel ${event.id} (${event.eventType}) encountered non-retryable error, moving to DLQ: $error"
    )

    db.run(outboxRepo.setError(event.id, error)).flatMap { _ =>
      moveToDLQ(event, "NON_RETRYABLE_ERROR", error, triggerProcessor = !isRevertEvent)
        .map(_ => false)
    }
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
