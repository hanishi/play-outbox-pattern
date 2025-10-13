package actors

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import publishers.EventPublisher
import repositories.OutboxRepository
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.duration.*

object OutboxProcessorRouter {

  /**
   * Creates a pool of OutboxProcessor workers.
   *
   * Benefits:
   * - Parallel processing across multiple workers
   * - Each worker independently fetches and processes events
   * - FOR UPDATE SKIP LOCKED prevents workers from grabbing same events
   * - Better CPU utilization and throughput
   *
   * @param poolSize Number of parallel workers (default: number of CPU cores)
   */
  def apply(
      db: Database,
      publisher: EventPublisher,
      outboxRepo: OutboxRepository,
      pollInterval: FiniteDuration = 5.seconds,
      batchSize: Int = 100,
      poolSize: Int = Runtime.getRuntime.availableProcessors()
  ): Behavior[OutboxProcessor.Command] = Behaviors.setup { context =>

    context.log.info(s"Starting OutboxProcessor pool with $poolSize workers")

    Routers.pool(poolSize) {
      OutboxProcessor(db, publisher, outboxRepo, pollInterval, batchSize)
    }.withRoundRobinRouting()
  }
}