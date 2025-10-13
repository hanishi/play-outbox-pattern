package repositories

import models.{ DomainEvent, OutboxEvent }
import repositories.OutboxEventsPostgresProfile.api.*
import slick.dbio.DBIO
import slick.lifted.TableQuery

import scala.concurrent.*

trait OutboxHelper {
  protected val outbox = TableQuery[OutboxTable]

  /** Executes an action and saves an event atomically within the same transaction.
    *
    * This method ensures that both the business action and the event are committed together,
    * maintaining the core guarantee of the outbox pattern: events are only persisted if
    * the business operation succeeds.
    *
    * @param event The domain event to save
    * @param action The database action to execute
    * @return The result of the action, with the event saved transactionally
    */
  protected def withEvent[T](
      event: DomainEvent
  )(action: DBIO[T])(using ec: ExecutionContext): DBIO[T] =
    (for {
      result <- action
      _ <- saveEvent(event)
    } yield result).transactionally

  /** Variant that allows creating the event based on the action's result.
    *
    * Useful when you need data from the action (like a generated ID) to construct the event.
    *
    * @param action The database action to execute
    * @param eventFactory Function that creates an event from the action's result
    * @return The result of the action, with the event saved transactionally
    */
  protected def withEventFactory[T](
      action: DBIO[T]
  )(eventFactory: T => DomainEvent)(using ec: ExecutionContext): DBIO[T] =
    (for {
      result <- action
      _ <- saveEvent(eventFactory(result))
    } yield result).transactionally

  private def saveEvent(event: DomainEvent): DBIO[Long] =
    (outbox returning outbox.map(_.id)) += OutboxEvent(
      aggregateId = event.aggregateId,
      eventType   = event.eventType,
      payloads    = event.toPayloads // Destination-specific payloads from domain event
    ).withIdempotencyKey
}
