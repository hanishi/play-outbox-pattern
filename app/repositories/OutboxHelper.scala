package repositories

import models.{ DomainEvent, OutboxEvent }
import slick.dbio.DBIO
import slick.jdbc.PostgresProfile.api.*
import slick.lifted.TableQuery

import scala.concurrent.*

trait OutboxHelper {
  protected val outbox = TableQuery[OutboxTable]

  protected def withEvent[T](
      event: DomainEvent
  )(action: DBIO[T])(using ec: ExecutionContext): DBIO[T] =
    (for {
      result <- action
      _ <- saveEvent(event)
    } yield result).transactionally

  protected def saveEvent(event: DomainEvent): DBIO[Long] =
    (outbox returning outbox.map(_.id)) += OutboxEvent(
      aggregateId = event.aggregateId,
      eventType   = event.eventType,
      payload     = event.toJson.toString
    )
}
