package services

import models.{ Order, OrderWithResults }
import repositories.{ DestinationResultRepository, OrderRepository }
import slick.jdbc.JdbcBackend.Database

import javax.inject.*
import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class OrderService @Inject() (
    orderRepo: OrderRepository,
    resultRepo: DestinationResultRepository
)(using ec: ExecutionContext, db: Database) {

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

  def listOrdersWithResults(limit: Int = 50, offset: Int = 0): Future[Seq[OrderWithResults]] = {
    import slick.jdbc.PostgresProfile.api.*

    val aggregateIdsQuery = sql"""
      SELECT ar.aggregate_id
      FROM aggregate_results ar
      INNER JOIN orders o ON CAST(ar.aggregate_id AS BIGINT) = o.id
      WHERE o.deleted = false
      GROUP BY ar.aggregate_id
      ORDER BY MAX(ar.published_at) DESC
      LIMIT $limit OFFSET $offset
    """.as[String]

    for {
      aggregateIds <- db.run(aggregateIdsQuery)
      orderIds = aggregateIds.map(_.toLong)
      ordersWithResults <- Future.sequence(orderIds.map { orderId =>
        for {
          orderOpt <- db.run(orderRepo.findById(orderId))
          results <- db.run(resultRepo.findByAggregateId(orderId.toString))
        } yield orderOpt.map(order => OrderWithResults(order, results))
      })
    } yield ordersWithResults.flatten
  }

  def deleteOrder(orderId: Long): Future[Int] =
    db.run(orderRepo.markDeleted(orderId))
}
