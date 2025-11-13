package services

import models.{ Order, OrderWithResults }
import repositories.{ DestinationResultRepository, OrderRepository }
import slick.jdbc.JdbcBackend.Database

import javax.inject.*
import scala.concurrent.{ ExecutionContext, Future }

/** Service layer for order operations with transactional outbox event publishing.
  *
  * == Purpose ==
  * Business logic layer that orchestrates order lifecycle operations while ensuring all state
  * changes are atomically persisted with their corresponding outbox events. Delegates to
  * OrderRepository for transactional guarantees via OutboxHelper pattern.
  *
  * == Architecture ==
  *
  * '''Controller → Service → Repository → Database'''
  * {{{
  * # 1. Controller receives HTTP request
  * POST /api/orders
  * Body: {"customerId": "C-123", "totalAmount": 99.99, "shippingType": "domestic"}
  *
  * # 2. OrderController calls OrderService.createOrder
  * orderService.createOrder(order)
  *
  * # 3. OrderService delegates to OrderRepository
  * db.run(orderRepo.createWithEvent(order))
  *
  * # 4. OrderRepository uses OutboxHelper for transactional write
  * BEGIN TRANSACTION
  *   INSERT INTO orders (...) RETURNING id;  -- Returns orderId: 123
  *   INSERT INTO outbox_events (
  *     aggregate_id = '123',
  *     event_type = 'OrderCreated',
  *     payloads = '{"inventory": {...}, "fraudCheck": {...}, "shipping": {...}, "billing": {...}}',
  *     status = 'PENDING'
  *   );
  * COMMIT
  *
  * # 5. OutboxProcessor picks up event and publishes to external APIs
  * POST /api/inventory/reserve       → Success
  * POST /api/fraud/check             → Success (riskScore: 25)
  * POST /api/domestic-shipping       → Success
  * POST /api/billing                 → Failure (after 3 retries)
  *
  * # 6. DLQProcessor automatically compensates successful calls (LIFO)
  * POST /api/domestic-shipping/{shipmentId}/cancel  → Success
  * DELETE /api/inventory/{reservationId}/release    → Success
  * }}}
  *
  * == Transactional Guarantees ==
  *
  * '''Atomic Persistence:'''
  * {{{
  * # All state-changing operations use OrderRepository's OutboxHelper methods:
  * # - createWithEvent: Order + OrderCreatedEvent saved together
  * # - updateStatusWithEvent: Status update + OrderStatusUpdatedEvent saved together
  * # - cancelWithEvent: Cancellation + OrderCancelledEvent saved together
  *
  * # Example transaction (create order):
  * BEGIN TRANSACTION
  *   INSERT INTO orders (customer_id, total_amount, shipping_type, order_status)
  *   VALUES ('C-123', 99.99, 'domestic', 'PENDING')
  *   RETURNING id;  -- Returns 123
  *
  *   INSERT INTO outbox_events (aggregate_id, event_type, payloads, status, idempotency_key)
  *   VALUES ('123', 'OrderCreated', '{"inventory": {...}}', 'PENDING', '123:OrderCreated');
  * COMMIT
  *
  * # If either INSERT fails, entire transaction rolls back
  * # If both succeed, event is guaranteed to be published (even after app crash)
  * }}}
  *
  * == Event Flow ==
  *
  * '''1. Order Creation (createOrder):'''
  * {{{
  * # User creates order via API
  * orderService.createOrder(Order(...))
  *
  * # Event published: OrderCreated
  * # Fanout destinations (from application.conf):
  * #   OrderCreated = ["inventory", "fraudCheck", "shipping", "billing"]
  *
  * # Forward flow (sequential processing):
  * POST /api/inventory/reserve       → 200 OK (reservationId: RES-456)
  * POST /api/fraud/check             → 200 OK (riskScore: 25)
  * POST /api/domestic-shipping       → 200 OK (shipmentId: SHIP-789)
  * POST /api/billing                 → 503 Service Unavailable (billing down)
  *
  * # Retry attempts (exponential backoff: 2s, 4s, 8s)
  * # After 3 retries → Move to DLQ
  *
  * # Automatic compensation (DLQProcessor):
  * # Query successful results: [shipping(2), fraudCheck(1), inventory(0)]  -- LIFO order
  * POST /api/domestic-shipping/SHIP-789/cancel  → 200 OK (shipment cancelled)
  * # fraudCheck skipped (read-only, no revert config)
  * DELETE /api/inventory/RES-456/release        → 200 OK (inventory released)
  *
  * # Result: Order created but external operations compensated
  * # Database: Order with status PENDING, all events tracked in aggregate_results
  * }}}
  *
  * '''2. Status Update (updateOrderStatus):'''
  * {{{
  * # Warehouse marks order as shipped
  * orderService.updateOrderStatus(orderId = 123, status = "SHIPPED")
  *
  * # Event published: OrderStatusUpdatedEvent
  * # Fanout destinations:
  * #   OrderStatusUpdated = ["notifications"]
  *
  * # Conditional routing based on newStatus field:
  * # newStatus = "SHIPPED" → POST /api/notifications/shipping-confirmation
  * # newStatus = "DELIVERED" → POST /api/notifications/review-request
  *
  * # Customer receives tracking email
  * }}}
  *
  * '''3. Order Cancellation (cancelOrder):'''
  * {{{
  * # User cancels order before shipment
  * orderService.cancelOrder(orderId = 123, reason = "Customer requested")
  *
  * # Event published: OrderCancelledEvent (eventType = "!OrderCreated")
  * # The "!" prefix triggers revert flow in OutboxProcessor
  *
  * # Query aggregate_results for successful forward calls:
  * SELECT * FROM aggregate_results
  * WHERE aggregate_id = '123'
  *   AND event_type = 'OrderCreated'
  *   AND success = true
  * ORDER BY fanout_order DESC;  -- LIFO order
  * # Returns: [billing(3), shipping(2), fraudCheck(1), inventory(0)]
  *
  * # Build and call revert endpoints (LIFO order):
  * POST /api/billing/{transactionId}/refund           → 200 OK
  * POST /api/domestic-shipping/{shipmentId}/cancel    → 200 OK
  * # fraudCheck skipped (no revert config)
  * DELETE /api/inventory/{reservationId}/release      → 200 OK
  *
  * # Database update:
  * UPDATE orders SET order_status = 'CANCELLED', updated_at = NOW() WHERE id = 123;
  *
  * # Result: Order cancelled, all operations reverted
  * }}}
  *
  * == Query Operations ==
  *
  * '''Read-only operations (no events published):'''
  * - `getOrder`: Retrieve single order by ID
  * - `listOrders`: Paginated list of orders
  * - `listOrdersWithResults`: Orders with complete API call audit trail
  *
  * '''listOrdersWithResults use cases:'''
  * {{{
  * # Use case 1: Customer support dashboard
  * # Shows order processing status with complete API call history
  * Order #123:
  *   - inventory.reserve: Success (reservationId: RES-456)
  *   - fraudCheck.check: Success (riskScore: 25)
  *   - shipping.create: Success (shipmentId: SHIP-789)
  *   - billing.charge: Failed (HTTP 503)
  *   - shipping.revert: Success (shipment cancelled)
  *   - inventory.revert: Success (inventory released)
  *
  * # Use case 2: Debugging failed orders
  * # Identifies which API call failed and why
  * # Shows request/response payloads for troubleshooting
  * }}}
  *
  * == Error Handling ==
  *
  * '''Repository Layer Errors:'''
  * {{{
  * # Order not found:
  * updateOrderStatus(orderId = 999, status = "SHIPPED")
  * # → Future fails with NoSuchElementException("Order 999 not found")
  * # → Controller returns 404 Not Found
  *
  * # Database constraint violation:
  * createOrder(Order(id = Some(123)))  // Duplicate primary key
  * # → Transaction rolls back
  * # → No order saved, no event published
  * # → Future fails with SQLException
  * }}}
  *
  * '''Processing Errors (OutboxProcessor):'''
  * {{{
  * # Handled automatically by OutboxProcessor:
  * # - Retryable failures: Exponential backoff, max 3 retries
  * # - Non-retryable failures: Immediate DLQ
  * # - Max retries exceeded: Move to DLQ, trigger automatic compensation
  * # - Service returns this method before processing starts (async)
  * }}}
  *
  * == Integration with Outbox Pattern ==
  *
  * '''Event Lifecycle:'''
  * {{{
  * # 1. Service method called (e.g., createOrder)
  * orderService.createOrder(order)
  *
  * # 2. Repository persists order + event atomically
  * # 3. Service method returns Future[Long] with order ID
  * # 4. Controller responds to user: {"orderId": 123}
  *
  * # 5. Asynchronously (milliseconds later):
  * #    - PostgreSQL NOTIFY triggers OutboxProcessor
  * #    - OutboxProcessor claims event (FOR UPDATE SKIP LOCKED)
  * #    - Publishes to external APIs (inventory → fraudCheck → shipping → billing)
  * #    - Saves results to aggregate_results table
  * #    - Marks event as PROCESSED or moves to DLQ
  *
  * # 6. If moved to DLQ:
  * #    - DLQProcessor automatically claims DLQ event
  * #    - Queries successful forward results
  * #    - Performs compensation in LIFO order
  * #    - Marks DLQ event as PROCESSED
  * }}}
  */
@Singleton
class OrderService @Inject() (
    orderRepo: OrderRepository,
    resultRepo: DestinationResultRepository
)(using ec: ExecutionContext, db: Database) {

  /** Creates a new order and publishes an "OrderCreated" event transactionally.
    *
    * The event will fan out to multiple destinations (billing, shipping, inventory).
    * If any destination fails after max retries, the event moves to DLQ and
    * automatic compensation reverts successful calls.
    *
    * @param order The order to create
    * @return Future containing the generated order ID
    */
  def createOrder(order: Order): Future[Long] =
    db.run(orderRepo.createWithEvent(order))

  /** Retrieves an order by ID.
    *
    * @param id The order ID
    * @return Future containing the order if found
    */
  def getOrder(id: Long): Future[Option[Order]] =
    db.run(orderRepo.findById(id))

  /** Updates an order's status and publishes an "OrderStatusUpdated" event transactionally.
    *
    * @param orderId The ID of the order to update
    * @param status The new status (e.g., "SHIPPED", "DELIVERED")
    * @return Future containing the number of rows updated (1 if successful)
    */
  def updateOrderStatus(orderId: Long, status: String): Future[Int] =
    db.run(orderRepo.updateStatusWithEvent(orderId, status))

  /** Cancels an order and publishes a "!OrderCreated" event to trigger automatic compensation.
    *
    * The "!" prefix triggers revert flow:
    * 1. Query aggregate_results for successful forward calls
    * 2. Reverse order (LIFO): [inventory, shipping, billing]
    * 3. Build revert endpoints from forward request/response data
    * 4. Call revert APIs (refund, cancel, release)
    *
    * @param orderId The ID of the order to cancel
    * @param reason Cancellation reason (default: "User requested")
    * @return Future containing the number of rows updated (1 if successful)
    */
  def cancelOrder(orderId: Long, reason: String = "User requested"): Future[Int] =
    db.run(orderRepo.cancelWithEvent(orderId, reason))

  /** Lists orders with pagination.
    *
    * @param limit Maximum number of orders to return (default: 50)
    * @param offset Number of orders to skip (default: 0)
    * @return Future containing the list of orders
    */
  def listOrders(limit: Int = 50, offset: Int = 0): Future[Seq[Order]] =
    db.run(orderRepo.list(limit, offset))

  /** Lists orders with their aggregate results (API call history).
    *
    * Returns orders that have API results, joined with their complete audit trail.
    * Useful for debugging, monitoring, and showing users the status of their order processing.
    *
    * @param limit Maximum number of orders to return (default: 50)
    * @param offset Number of orders to skip (default: 0)
    * @return Future containing orders with their API call results
    */
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

  /** Soft-deletes an order by marking it as deleted (does not physically remove from database).
    *
    * Note: This does not publish an event or trigger compensation. If you need to revert
    * the order's API calls, use cancelOrder() instead.
    *
    * @param orderId The ID of the order to delete
    * @return Future containing the number of rows updated (1 if successful)
    */
  def deleteOrder(orderId: Long): Future[Int] =
    db.run(orderRepo.markDeleted(orderId))
}
