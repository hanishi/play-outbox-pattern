package actors

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import publishers.EventPublisher
import repositories.OutboxRepository
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.duration.*

object OutboxProcessorRouter {

  def apply(
      db: Database,
      publisher: EventPublisher,
      outboxRepo: OutboxRepository,
      pollInterval: FiniteDuration = 5.seconds,
      batchSize: Int               = 100,
      poolSize: Int                = Runtime.getRuntime.availableProcessors(),
      maxRetries: Int              = 2,
      useListenNotify: Boolean     = false
  ): Behavior[OutboxProcessor.Command] = Behaviors.setup { context =>

    context.log.info(s"Starting OutboxProcessor pool with $poolSize workers")

    Routers
      .pool(poolSize) {
        OutboxProcessor(
          db,
          publisher,
          outboxRepo,
          pollInterval,
          batchSize,
          maxRetries,
          useListenNotify
        )
      }
      .withRoundRobinRouting()
  }
}
