package actors

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import play.api.Logger

import scala.concurrent.*
import scala.concurrent.duration.*
import scala.util.{ Failure, Success }

object EventProcessor {

  trait Base[Command] {
    protected def logger: Logger
    protected def context: ActorContext[Command]
    protected def timers: TimerScheduler[Command]
    protected def pollInterval: FiniteDuration

    protected given ExecutionContext = context.executionContext

    /** Schedule the next processing cycle */
    protected def scheduleNext(triggerCommand: Command): Unit =
      timers.startSingleTimer(triggerCommand, pollInterval)

    /** Pipe a future result back to self as a command */
    protected def pipeToSelf[T](
        fut: => Future[T]
    )(onSuccess: T => Command, onFailure: Throwable => Command): Unit =
      context.pipeToSelf(fut) {
        case Success(v) => onSuccess(v)
        case Failure(ex) => onFailure(ex)
      }

    /** Generic batch processing pattern used by both processors.
      *
      * Fetches a batch of events, processes each one individually,
      * and aggregates the results into Stats.
      *
      * @param fetchBatch Function to fetch events from table(s)
      * @param processEvent Function to process a single event
      * @param eventTypeName Name for logging (e.g., "outbox", "DLQ")
      * @tparam E Event type
      */
    protected def processBatch[E](
        fetchBatch: => Future[Seq[E]],
        processEvent: E => Future[Boolean],
        eventTypeName: String,
        getEventId: E => Long
    ): Future[Stats] = {
      logger.info(s"Fetching $eventTypeName events")
      fetchBatch
        .flatMap { events =>
          val fetchedCount = events.length
          logger.info(s"Fetched $fetchedCount $eventTypeName events from database")

          if (fetchedCount > 0) {
            // Avoid building huge strings! cap to 50 IDs and use DEBUG level
            val sampleIds = events.map(getEventId).take(50).mkString(", ")
            logger.debug(s"Event IDs (up to 50): $sampleIds")
          }

          if (events.isEmpty) Future.successful(Stats(0))
          else
            Future.traverse(events)(processEvent).map { results =>
              results.foldLeft(Stats(fetchedCount))(_ + _)
            }
        }
        .recoverWith { case ex =>
          logger.error(s"Error fetching $eventTypeName events", ex)
          Future.failed(ex)
        }
    }
  }

  /** Common stats tracking for batch processing */
  final case class Stats(processed: Int, failed: Int, fetched: Int) {
    def +(success: Boolean): Stats =
      if (success) copy(processed = processed + 1) else copy(failed = failed + 1)
  }

  object Stats {
    def apply(fetched: Int): Stats = Stats(0, 0, fetched)
  }
}
