package repositories

import models.{ Order, OrderCancelledEvent, OrderCreatedEvent, OrderStatusUpdatedEvent }
import repositories.OutboxEventsPostgresProfile.api.*

import java.time.Instant
import javax.inject.*
import scala.concurrent.ExecutionContext

class OrderTable(tag: Tag) extends Table[Order](tag, "orders") {
  def * = (id, customerId, totalAmount, shippingType, orderStatus, createdAt, updatedAt, deleted)
    .mapTo[Order]

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def customerId = column[String]("customer_id")

  def totalAmount = column[BigDecimal]("total_amount")

  def shippingType = column[String]("shipping_type")

  def orderStatus = column[String]("order_status")

  def createdAt = column[Instant]("created_at")

  def updatedAt = column[Instant]("updated_at")

  def deleted = column[Boolean]("deleted")
}

@Singleton
class OrderRepository @Inject() ()(using ec: ExecutionContext) extends OutboxHelper {
  private val orders                            = TableQuery[OrderTable]
  def createWithEvent(order: Order): DBIO[Long] =
    withEventFactory((orders returning orders.map(_.id)) += order) { orderId =>
      OrderCreatedEvent(
        orderId      = orderId,
        customerId   = order.customerId,
        totalAmount  = order.totalAmount,
        shippingType = order.shippingType
      )
    }

  def updateStatusWithEvent(orderId: Long, newStatus: String): DBIO[Int] =
    for {
      orderOpt <- findById(orderId)
      order <- orderOpt match {
        case Some(o) => DBIO.successful(o)
        case None => DBIO.failed(new NoSuchElementException(s"Order $orderId not found"))
      }
      updated <- withEvent(
        OrderStatusUpdatedEvent(
          orderId   = orderId,
          oldStatus = order.orderStatus,
          newStatus = newStatus
        )
      ) {
        orders
          .filter(_.id === orderId)
          .map(o => (o.orderStatus, o.updatedAt))
          .update((newStatus, Instant.now()))
      }
    } yield updated

  def findById(id: Long): DBIO[Option[Order]] =
    orders.filter(o => o.id === id && o.deleted === false).result.headOption

  def cancelWithEvent(orderId: Long, reason: String): DBIO[Int] =
    for {
      orderOpt <- findById(orderId)
      _ <- orderOpt match {
        case Some(_) => DBIO.successful(())
        case None => DBIO.failed(new NoSuchElementException(s"Order $orderId not found"))
      }
      updated <- withEvent(
        OrderCancelledEvent(
          orderId = orderId,
          reason  = reason
        )
      ) {
        orders
          .filter(_.id === orderId)
          .map(o => (o.orderStatus, o.updatedAt))
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
