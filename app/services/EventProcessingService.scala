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

/** Event processing service that bootstraps and manages OutboxProcessor actors.
  *
  * == Purpose ==
  * Lifecycle manager for the transactional outbox pattern processing infrastructure.
  * Starts OutboxProcessor actors at application startup and ensures graceful shutdown.
  *
  * == Architecture ==
  *
  * '''Single Processor Mode (outbox.poolSize = 1):'''
  * {{{
  * # Configuration
  * outbox {
  *   poolSize = 1
  *   batchSize = 100
  *   maxRetries = 3
  * }
  *
  * # Actor Hierarchy
  * akka://application/user/outbox-processor
  *   └── akka://application/user/outbox-processor/dlq-processor
  *
  * # Processing Flow
  * 1. OutboxProcessor claims events with FOR UPDATE SKIP LOCKED
  * 2. Publishes to external APIs (inventory → fraudCheck → shipping → billing)
  * 3. On max retries exceeded → moves to DLQ
  * 4. DLQProcessor performs automatic compensation (LIFO order)
  * }}}
  *
  * '''Pool Mode (outbox.poolSize > 1):'''
  * {{{
  * # Configuration
  * outbox {
  *   poolSize = 3  # 3 concurrent workers
  *   batchSize = 100
  *   maxRetries = 3
  * }
  *
  * # Actor Hierarchy
  * akka://application/user/outbox-processor-pool (router)
  *   ├── akka://application/user/outbox-processor-pool/$a (worker 1)
  *   │     └── dlq-processor (child)
  *   ├── akka://application/user/outbox-processor-pool/$b (worker 2)
  *   │     └── dlq-processor (child)
  *   └── akka://application/user/outbox-processor-pool/$c (worker 3)
  *         └── dlq-processor (child)
  *
  * # Processing Flow
  * 1. Router distributes ProcessUnhandledEvent to workers (round-robin)
  * 2. Each worker claims events independently (FOR UPDATE SKIP LOCKED prevents overlap)
  * 3. Parallel processing: Worker A handles events [1,2,3], Worker B handles [4,5,6], etc.
  * 4. Each worker has its own DLQ processor for compensation
  * }}}
  *
  * == PostgreSQL LISTEN/NOTIFY ==
  *
  * '''Enabled (outbox.useListenNotify = true):'''
  * {{{
  * # Near-instant event processing
  * # 1. Repository inserts event into outbox_events table
  * # 2. Database trigger sends: NOTIFY outbox_event_inserted, '123'
  * # 3. OutboxProcessor receives notification immediately
  * # 4. Processes event within milliseconds (no polling delay)
  *
  * # Fallback: If batch is full (100 events), immediate reprocessing (100ms)
  * # Fallback: If event has next_retry_at in the future, schedule exact wakeup
  * }}}
  *
  * '''Disabled (outbox.useListenNotify = false):'''
  * {{{
  * # Polling mode (outbox.pollInterval = 2 seconds)
  * # 1. Repository inserts event into outbox_events table
  * # 2. OutboxProcessor polls every 2 seconds
  * # 3. Processes event with up to 2 second latency
  * }}}
  *
  * == Stale Event Recovery ==
  *
  * '''Problem:'''
  * {{{
  * # Scenario: Worker crashes while processing event
  * # 1. Event claimed: UPDATE outbox_events SET status='PROCESSING' WHERE id=123
  * # 2. Worker crashes (network partition, OOM, etc.)
  * # 3. Event stuck in PROCESSING state forever (no worker will pick it up)
  * }}}
  *
  * '''Solution: Automatic Cleanup'''
  * {{{
  * # Configuration
  * outbox {
  *   enableStaleEventCleanup = true
  *   staleEventTimeoutMinutes = 5
  *   cleanupInterval = 1 minute
  * }
  *
  * # Cleanup Flow
  * # Every 1 minute:
  * # SELECT id FROM outbox_events
  * # WHERE status = 'PROCESSING'
  * #   AND status_changed_at < NOW() - INTERVAL '5 minutes'
  *
  * # Reset to PENDING:
  * # UPDATE outbox_events SET status='PENDING' WHERE id IN (...)
  *
  * # Next poll cycle picks up the event and retries
  * }}}
  *
  * == Lifecycle ==
  *
  * '''Startup:'''
  * {{{
  * # 1. Application starts
  * # 2. EventProcessingService @Singleton created (eager initialization via Play DI)
  * # 3. Spawns OutboxProcessor actor(s) based on poolSize
  * # 4. Each OutboxProcessor spawns child DLQProcessor
  * # 5. Sends initial ProcessUnhandledEvent message to trigger processing
  * # 6. If useListenNotify=true, establishes PostgreSQL LISTEN connection
  * }}}
  *
  * '''Shutdown:'''
  * {{{
  * # 1. Application receives SIGTERM
  * # 2. Play ApplicationLifecycle triggers shutdown hooks
  * # 3. Sends Stop message to OutboxProcessor (with 10s timeout)
  * # 4. OutboxProcessor stops child DLQProcessor
  * # 5. All timers cancelled, in-flight processing completes
  * # 6. Database connections closed
  * }}}
  *
  * == Example Configuration ==
  * {{{
  * # conf/application.conf
  * outbox {
  *   pollInterval = 2 seconds         # Polling interval when LISTEN/NOTIFY disabled
  *   batchSize = 100                  # Max events to process per batch
  *   poolSize = 3                     # Number of concurrent workers (1 = single processor)
  *   maxRetries = 3                   # Retry attempts before moving to DLQ
  *   useListenNotify = true           # Enable PostgreSQL LISTEN/NOTIFY for instant processing
  *   enableStaleEventCleanup = true   # Enable automatic recovery of stuck events
  *   staleEventTimeoutMinutes = 5     # Consider events stale after 5 minutes
  *   cleanupInterval = 1 minute       # Run cleanup check every minute
  *
  *   dlq {
  *     maxRetries = 3                 # Retry attempts for compensation operations
  *     pollInterval = 2 seconds       # Polling interval for DLQ events
  *   }
  * }
  *
  * outbox.http.fanout {
  *   OrderCreated = ["inventory", "fraudCheck", "shipping", "billing"]
  * }
  * }}}
  *
  * == Monitoring ==
  *
  * '''Logs:'''
  * {{{
  * # Startup
  * [info] Starting outbox processor with pool router (size: 3)
  * [info] Started DLQ processor as child actor (maxRetries: 3)
  * [info] Event processing started (pool: 3, batch: 100, useListenNotify: true)
  *
  * # Processing
  * [info] Batch complete - Fetched: 50, Processed: 48, Failed: 2
  * [info] Event 123 exceeded retries, moving to DLQ
  * [info] DLQ Batch complete - Fetched: 1, Reverted: 1, Failed: 0
  *
  * # Stale cleanup
  * [warn] Found 2 stale PROCESSING events (timeout: 5 minutes), resetting to PENDING
  * [info] Successfully reset 2 stale events to PENDING
  *
  * # Shutdown
  * [info] Stopping event processors
  * [info] Outbox processor (and child DLQ processor) stopped
  * }}}
  */
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

  private given Timeout = Timeout(10.seconds)
}
