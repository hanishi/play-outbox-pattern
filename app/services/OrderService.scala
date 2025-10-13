package services

import models.Order
import repositories.OrderRepository
import slick.jdbc.JdbcBackend.Database

import javax.inject.*
import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class OrderService @Inject() (
    db: Database,
    orderRepo: OrderRepository,
    outboxService: OutboxService
)(implicit ec: ExecutionContext) {

  def createOrder(order: Order): Future[Long] =
    db.run(orderRepo.createWithEvent(order))

  def getOrder(id: Long): Future[Option[Order]] =
    db.run(orderRepo.findById(id))

  def updateOrderStatus(orderId: Long, status: String): Future[Int] =
    db.run(orderRepo.updateStatusWithEvent(orderId, status))

  def cancelOrder(orderId: Long, reason: String = "User requested"): Future[Int] =
    db.run(orderRepo.cancelWithEvent(orderId, reason))

  def listOrders(limit: Int = 50, offset: Int = 0): Future[Seq[Order]] =
    db.run(orderRepo.list(limit, offset))

  def deleteOrder(orderId: Long): Future[Int] =
    db.run(orderRepo.markDeleted(orderId))
}
