package repositories

import models.{ Order, OrderCancelledEvent, OrderCreatedEvent, OrderStatusUpdatedEvent }
import slick.jdbc.PostgresProfile.api.*

import java.time.Instant
import javax.inject.*
import scala.concurrent.ExecutionContext

class OrderTable(tag: Tag) extends Table[Order](tag, "orders") {
  def * = (id, customerId, totalAmount, status, createdAt, updatedAt, deleted)
    .mapTo[Order]

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def customerId = column[String]("customer_id")

  def totalAmount = column[BigDecimal]("total_amount")

  def status = column[String]("status")

  def createdAt = column[Instant]("created_at")

  def updatedAt = column[Instant]("updated_at")

  def deleted = column[Boolean]("deleted")
}

@Singleton
class OrderRepository @Inject() ()(using ec: ExecutionContext) extends OutboxHelper {
  private val orders                                    = TableQuery[OrderTable]
  def createWithEvent(order: Order): DBIO[Long] =
    for {
      orderId <- (orders returning orders.map(_.id)) += order
      _ <- saveEvent(OrderCreatedEvent(
        orderId     = orderId,
        customerId  = order.customerId,
        totalAmount = order.totalAmount
      ))
    } yield orderId

  def updateStatusWithEvent(orderId: Long, newStatus: String): DBIO[Int] =
    for {
      orderOpt <- findById(orderId)
      order <- orderOpt match {
        case Some(o) => DBIO.successful(o)
        case None    => DBIO.failed(new NoSuchElementException(s"Order $orderId not found"))
      }
      updated <- withEvent(OrderStatusUpdatedEvent(
        orderId   = orderId,
        oldStatus = order.status,
        newStatus = newStatus
      )) {
        orders
          .filter(_.id === orderId)
          .map(o => (o.status, o.updatedAt))
          .update((newStatus, Instant.now()))
      }
    } yield updated

  def findById(id: Long): DBIO[Option[Order]] = orders.filter(_.id === id).result.headOption

  def cancelWithEvent(orderId: Long, reason: String): DBIO[Int] =
    for {
      orderOpt <- findById(orderId)
      _ <- orderOpt match {
        case Some(_) => DBIO.successful(())
        case None    => DBIO.failed(new NoSuchElementException(s"Order $orderId not found"))
      }
      updated <- withEvent(OrderCancelledEvent(
        orderId = orderId,
        reason  = reason
      )) {
        orders
          .filter(_.id === orderId)
          .map(o => (o.status, o.updatedAt))
          .update(("CANCELLED", Instant.now()))
      }
    } yield updated

  def list(limit: Int = 50, offset: Int = 0): DBIO[Seq[Order]] =
    orders
      .filter(_.deleted === false)
      .sortBy(_.createdAt.desc)
      .drop(offset)
      .take(limit)
      .result

  def markDeleted(orderId: Long): DBIO[Int] =
    orders
      .filter(_.id === orderId)
      .map(_.deleted)
      .update(true)
}
