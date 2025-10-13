package actors

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import publishers.EventPublisher
import repositories.OutboxRepository
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.duration.*

object OutboxProcessorRouter {

  def apply(
      publisher: EventPublisher,
      outboxRepo: OutboxRepository,
      dlqRepo: repositories.DeadLetterRepository,
      resultRepo: repositories.DestinationResultRepository,
      eventRouter: publishers.EventRouter,
      pollInterval: FiniteDuration    = 5.seconds,
      batchSize: Int                  = 100,
      poolSize: Int                   = Runtime.getRuntime.availableProcessors(),
      maxRetries: Int                 = 2,
      useListenNotify: Boolean        = false,
      staleCleanupEnabled: Boolean    = true,
      staleTimeoutMinutes: Int        = 5,
      cleanupInterval: FiniteDuration = 1.minute
  )(using db: Database): Behavior[OutboxProcessor.Command] = Behaviors.setup { context =>

    context.log.info(s"Starting OutboxProcessor pool with $poolSize workers")

    Routers
      .pool(poolSize) {
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
        )
      }
      .withRoundRobinRouting()
  }
}
