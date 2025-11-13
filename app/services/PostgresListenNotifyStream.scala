package services

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ ActorSystem, DispatcherSelector }
import org.apache.pekko.stream.scaladsl.{ Keep, RestartSource, Source }
import org.apache.pekko.stream.{ KillSwitches, RestartSettings, UniqueKillSwitch }
import org.postgresql.PGConnection
import play.api.Logger
import slick.jdbc.JdbcBackend.Database

import java.sql.Connection
import scala.concurrent.duration.*
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.Exception.*
import scala.util.{ Try, Using }

/** PostgreSQL LISTEN/NOTIFY stream implementation using Pekko Streams.
  *
  * == Purpose ==
  * Creates a resilient, self-healing Pekko Stream that listens to PostgreSQL NOTIFY events and
  * converts them into typed ChannelNotification messages for event-driven processing.
  *
  * == Architecture ==
  *
  * '''PostgreSQL Trigger → NOTIFY → JDBC Connection → Pekko Stream → NotificationService'''
  * '''PostgreSQL Trigger → NOTIFY → JDBC Connection → Pekko Stream → NotificationService'''
  * {{{
  * # 1. Database trigger fires on INSERT
  * CREATE TRIGGER outbox_events_notify
  * AFTER INSERT ON outbox_events
  * FOR EACH ROW WHEN (NEW.status = 'PENDING')
  * EXECUTE FUNCTION notify_outbox_event();
  *
  * # 2. Trigger function sends NOTIFY
  * CREATE FUNCTION notify_outbox_event() RETURNS TRIGGER AS $$
  * BEGIN
  *   PERFORM pg_notify('outbox_events_channel', NEW.id::text);
  *   RETURN NEW;
  * END;
  * $$ LANGUAGE plpgsql;
  *
  * # 3. PostgresListenNotifyStream receives notification via JDBC
  * LISTEN outbox_events_channel;
  * # Polls: SELECT pg_notifications() every 200ms
  *
  * # 4. Stream emits ChannelNotification
  * ChannelNotification(channel = "outbox_events_channel", eventIds = [123, 456, 789])
  *
  * # 5. NotificationService routes to OutboxProcessor
  * outboxActor ! ProcessUnhandledEvent
  * }}}
  *
  * == Stream Lifecycle ==
  *
  * '''Startup:'''
  * {{{
  * # 1. NotificationService calls createChannelSource
  * # 2. Opens PostgreSQL connection on blocking dispatcher
  * # 3. Executes LISTEN for each configured channel:
  *      LISTEN outbox_events_channel;
  *      LISTEN dlq_events_channel;
  * # 4. Starts polling loop (getNotifications every 200ms)
  * # 5. Returns (UniqueKillSwitch, Future[Done]) for lifecycle control
  * }}}
  *
  * '''Runtime:'''
  * {{{
  * # Continuous polling loop:
  * while (stream active) {
  *   notifications = pg.getNotifications(200ms)  // Blocks up to 200ms
  *   if (notifications.nonEmpty) {
  *     grouped = notifications.groupBy(_.channel)
  *     emit ChannelNotification for each channel
  *   }
  * }
  *
  * # Example: 3 events inserted
  * NOTIFY outbox_events_channel, '123';
  * NOTIFY outbox_events_channel, '456';
  * NOTIFY outbox_events_channel, '789';
  *
  * # Stream batches and emits:
  * ChannelNotification(channel = "outbox_events_channel", eventIds = [123, 456, 789])
  * }}}
  *
  * '''Shutdown:'''
  * {{{
  * # 1. Application receives SIGTERM
  * # 2. NotificationService calls killSwitch.shutdown()
  * # 3. Stream completes current read
  * # 4. Executes UNLISTEN * to cleanup
  * # 5. Closes PostgreSQL connection
  * # 6. Future[Done] completes
  * }}}
  *
  * == Resilience & Error Handling ==
  *
  * '''Automatic Restart with Exponential Backoff:'''
  * {{{
  * # Configuration:
  * Config(
  *   restartMinBackoff = 500.millis,   # Initial retry delay
  *   restartMaxBackoff = 30.seconds,   # Maximum retry delay
  *   restartRandomFactor = 0.2         # Jitter to prevent thundering herd
  * )
  *
  * # Failure scenario: Database connection lost
  * # T+0s: Connection fails (network partition)
  * # T+0.5s: Restart attempt 1 (500ms backoff)
  * # T+1.5s: Restart attempt 2 (1s backoff)
  * # T+3.5s: Restart attempt 3 (2s backoff)
  * # T+7.5s: Restart attempt 4 (4s backoff)
  * # ... continues up to 30s max backoff
  *
  * # Once connection restored:
  * # Stream resumes, re-executes LISTEN commands, continues polling
  * }}}
  *
  * '''Error Recovery:'''
  * {{{
  * # Database connection errors:
  * # → Logged: "Error opening LISTEN connection: <exception>"
  * # → RestartSource automatically retries with backoff
  * # → Stream never completes (keeps retrying indefinitely)
  *
  * # Invalid notification data:
  * # NOTIFY outbox_events_channel, 'invalid';
  * # → Try(n.getParameter.toLong).toOption filters out non-numeric IDs
  * # → Invalid notifications silently dropped
  * # → Valid notifications still processed
  * }}}
  *
  * == Resource Management ==
  *
  * '''unfoldResourceAsync Pattern:'''
  * {{{
  * # Three lifecycle phases:
  *
  * # 1. Create: Open connection (runs on blocking dispatcher)
  * create = () => Future {
  *   val conn = db.source.createConnection()
  *   conn.setAutoCommit(true)
  *   conn.createStatement().execute("LISTEN outbox_events_channel")
  *   conn
  * }
  *
  * # 2. Read: Poll for notifications (runs on blocking dispatcher)
  * read = conn => Future {
  *   val pg = conn.unwrap(classOf[PGConnection])
  *   val notifications = pg.getNotifications(200)  // Blocks up to 200ms
  *   // Process and group notifications
  *   Some(channelNotifications)
  * }
  *
  * # 3. Close: Cleanup connection (runs on blocking dispatcher)
  * close = conn => Future {
  *   conn.createStatement().execute("UNLISTEN *")
  *   conn.close()
  *   Done
  * }
  *
  * # Guarantees:
  * # - Create called once per stream lifecycle
  * # - Read called repeatedly while stream active
  * # - Close called exactly once when stream completes/fails
  * # - All operations use blocking-jdbc dispatcher (not default dispatcher)
  * }}}
  *
  * == Performance Characteristics ==
  *
  * '''Latency:'''
  * {{{
  * # Best case (event arrives during poll):
  * # T+0ms: Event inserted, NOTIFY sent
  * # T+5ms: getNotifications() returns immediately
  * # T+10ms: Stream emits ChannelNotification
  * # Total: ~10ms latency
  *
  * # Worst case (event arrives just after poll):
  * # T+0ms: getNotifications() starts (no events)
  * # T+1ms: Event inserted, NOTIFY sent
  * # T+200ms: getNotifications() times out, returns empty
  * # T+201ms: Next getNotifications() starts
  * # T+205ms: getNotifications() returns with event
  * # Total: ~205ms latency (one full poll cycle)
  *
  * # Average latency: ~100ms (half of pollIntervalMs)
  * }}}
  *
  * '''Throughput:'''
  * {{{
  * # Batching behavior:
  * # All notifications received within a 200ms window are batched together
  * # Example: 1000 events/second = ~200 events per poll
  * # Stream emits: ChannelNotification(eventIds = [1..200])
  * # NotificationService triggers processor once for entire batch
  * }}}
  *
  * '''Resource Usage:'''
  * {{{
  * # Database connections: 1 per stream (persistent)
  * # Threads: 1 from blocking-jdbc dispatcher (blocking I/O)
  * # CPU: Minimal (mostly waiting in getNotifications())
  * # Memory: O(notifications per poll) - typically <1KB
  * }}}
  *
  * == Configuration ==
  *
  * '''Config Parameters:'''
  * {{{
  * Config(
  *   channels = Seq("outbox_events_channel", "dlq_events_channel"),
  *   pollIntervalMs = 200,               # How long to block waiting for notifications
  *   restartMinBackoff = 500.millis,     # Initial retry delay after failure
  *   restartMaxBackoff = 30.seconds,     # Maximum retry delay (with exponential backoff)
  *   restartRandomFactor = 0.2           # Jitter factor (±20%) to prevent synchronized retries
  * )
  * }}}
  *
  * '''Tuning Guidelines:'''
  * {{{
  * # Lower pollIntervalMs (e.g., 100ms):
  * # - Pros: Lower average latency (~50ms)
  * # - Cons: More CPU usage (more frequent polls)
  *
  * # Higher pollIntervalMs (e.g., 500ms):
  * # - Pros: Lower CPU usage
  * # - Cons: Higher average latency (~250ms)
  *
  * # Recommended: 200ms (good balance of latency vs CPU)
  * }}}
  *
  * == Example Usage ==
  * {{{
  * val config = PostgresListenNotifyStream.Config(
  *   channels = Seq("outbox_events_channel", "dlq_events_channel")
  * )
  *
  * val (killSwitch, doneFuture) = PostgresListenNotifyStream
  *   .createChannelSource(config)
  *   .map { notification =>
  *     notification.channel match {
  *       case "outbox_events_channel" =>
  *         logger.info(s"Processing ${notification.eventIds.length} outbox events")
  *         outboxActor ! ProcessUnhandledEvent
  *       case "dlq_events_channel" =>
  *         logger.info(s"Processing ${notification.eventIds.length} DLQ events")
  *         outboxActor ! TriggerDLQProcessing
  *     }
  *   }
  *   .toMat(Sink.ignore)(Keep.both)
  *   .run()
  *
  * // Graceful shutdown
  * lifecycle.addStopHook { () =>
  *   killSwitch.shutdown()
  *   doneFuture
  * }
  * }}}
  */
object PostgresListenNotifyStream {

  private val logger = Logger(this.getClass)

  /** Creates a resilient Pekko Stream source that listens to PostgreSQL NOTIFY events.
    *
    * The stream automatically handles connection failures with exponential backoff and restart.
    * Uses unfoldResourceAsync for proper resource management (open connection, poll, close).
    *
    * @param config Stream configuration (channels, poll interval, restart settings)
    * @param db Database instance for creating connections
    * @param system Actor system for dispatchers and stream execution
    * @return Source that emits ChannelNotification and provides UniqueKillSwitch for shutdown
    */
  def createChannelSource(
      config: Config
  )(using db: Database, system: ActorSystem[?]): Source[ChannelNotification, UniqueKillSwitch] = {

    logger.info(s"Creating LISTEN/NOTIFY source for channels: ${config.channels.mkString(", ")}")

    given blockingEc: ExecutionContext =
      system.dispatchers.lookup(DispatcherSelector.fromConfig("blocking-jdbc"))

    def openConnection(): Connection = {
      val conn = db.source.createConnection()
      conn.setAutoCommit(true)
      config.channels.foreach { channel =>
        Using.resource(conn.createStatement())(_.execute(s"LISTEN $channel"))
        logger.debug(s"LISTEN connection opened for channel: $channel")
      }
      conn
    }

    def closeConnection(conn: Connection): Done = {
      Try(Using.resource(conn.createStatement())(_.execute("UNLISTEN *")))
      Try(conn.close())
      Done
    }

    val restartSettings = RestartSettings(
      config.restartMinBackoff,
      config.restartMaxBackoff,
      config.restartRandomFactor
    )

    RestartSource
      .withBackoff(restartSettings) { () =>
        Source
          .unfoldResourceAsync[Seq[ChannelNotification], Connection](
            create = () =>
              Future {
                catching(classOf[Throwable])
                  .withApply { ex =>
                    logger.error(s"Error opening LISTEN connection: $ex", ex)
                    throw ex
                  }
                  .apply(openConnection())
              }(blockingEc),
            read = conn =>
              Future {
                val pg            = conn.unwrap(classOf[PGConnection])
                val notifications =
                  Option(pg.getNotifications(config.pollIntervalMs)).toList.flatten

                val byChannel = notifications
                  .filter(n => config.channels.contains(n.getName))
                  .groupBy(_.getName)
                  .map { case (channel, notifs) =>
                    val eventIds = notifs.flatMap(n => Try(n.getParameter.toLong).toOption)
                    ChannelNotification(channel, eventIds)
                  }
                  .filter(_.eventIds.nonEmpty)
                  .toSeq

                if (byChannel.nonEmpty) {
                  byChannel.foreach { cn =>
                    logger.info(
                      s"LISTEN $cn.channel: got ${cn.eventIds.length} event IDs: ${cn.eventIds.mkString(", ")}"
                    )
                  }
                }

                Some(byChannel)
              }(blockingEc),
            close = conn => Future(closeConnection(conn))(blockingEc)
          )
          .mapConcat(identity)
      }
      .viaMat(KillSwitches.single)(Keep.right)
  }

  /** Notification message emitted by the stream containing event IDs for a specific channel.
    *
    * @param channel The PostgreSQL notification channel name (e.g., "outbox_events_channel")
    * @param eventIds Sequence of event IDs from notifications (extracted from NOTIFY payload)
    */
  case class ChannelNotification(channel: String, eventIds: Seq[Long])

  /** Configuration for PostgreSQL LISTEN/NOTIFY stream.
    *
    * @param channels List of PostgreSQL channels to LISTEN on (e.g., ["outbox_events_channel", "dlq_events_channel"])
    * @param pollIntervalMs How long to block waiting for notifications in milliseconds (default: 200ms)
    * @param restartMinBackoff Initial backoff delay when restarting after failure (default: 500ms)
    * @param restartMaxBackoff Maximum backoff delay with exponential backoff (default: 30s)
    * @param restartRandomFactor Jitter factor to randomize restart delays (default: 0.2 = ±20%)
    */
  case class Config(
      channels: Seq[String],
      pollIntervalMs: Int               = 200,
      restartMinBackoff: FiniteDuration = 500.millis,
      restartMaxBackoff: FiniteDuration = 30.seconds,
      restartRandomFactor: Double       = 0.2
  )
}
