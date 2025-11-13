package repositories

import models.{ DomainEvent, OutboxEvent }
import repositories.OutboxEventsPostgresProfile.api.*
import slick.dbio.DBIO
import slick.lifted.TableQuery

import scala.concurrent.*

/** Mixin trait providing transactional outbox pattern support for repositories.
  *
  * == Purpose ==
  * Ensures business operations and event publishing happen atomically within the same database transaction.
  * This is the core implementation of the Transactional Outbox Pattern, guaranteeing events are never lost.
  *
  * == The Problem (Without Outbox Pattern) ==
  * {{{
  * # ❌ Naive approach: Events can be lost
  * def createOrder(order: Order): Future[Long] = {
  *   db.run(orders += order)  // Transaction 1: committed to database
  *   publisher.publish(OrderCreatedEvent)  // Separate call
  *   // Problem: If app crashes here, event is lost!
  *   // Problem: If publisher fails, database change is already committed!
  * }
  *
  * # Result: Database has order, but no event published → inventory never reserved, billing never charged
  * }}}
  *
  * == The Solution (Transactional Outbox Pattern) ==
  * {{{
  * # ✓ Outbox pattern: Events guaranteed
  * def createWithEvent(order: Order): DBIO[Long] =
  *   withEventFactory((orders returning orders.map(_.id)) += order) { orderId =>
  *     OrderCreatedEvent(orderId, ...)
  *   }
  *
  * # Transaction flow:
  * BEGIN TRANSACTION
  *   INSERT INTO orders (customer_id, total_amount, ...) VALUES (...)  -- Returns orderId
  *   INSERT INTO outbox_events (aggregate_id, event_type, payloads) VALUES (...)
  * COMMIT
  *
  * # Both succeed or both fail together!
  * # Even if app crashes after commit, OutboxProcessor will publish the event on restart
  * }}}
  *
  * == Guarantees ==
  * 1. '''Atomicity''': Business change and event committed together or rolled back together
  * 2. '''Durability''': Events persisted in database, survive application crashes
  * 3. '''Eventual Delivery''': OutboxProcessor ensures events are eventually published
  * 4. '''Exactly-once Publishing''': Idempotency keys prevent duplicate event publishing
  * 5. '''Ordering''': Events processed in insertion order (FIFO per aggregate)
  *
  * == Usage Patterns ==
  *
  * '''Pattern 1: Event Known Upfront (withEvent)'''
  * {{{
  * // Use when event can be created before the action
  * def updateStatusWithEvent(orderId: Long, newStatus: String): DBIO[Int] =
  *   withEvent(OrderStatusUpdatedEvent(orderId, oldStatus, newStatus)) {
  *     orders.filter(_.id === orderId).map(_.status).update(newStatus)
  *   }
  *
  * // Transaction:
  * BEGIN
  *   UPDATE orders SET status = 'SHIPPED' WHERE id = 123
  *   INSERT INTO outbox_events (event_type='OrderStatusUpdated', aggregate_id='123')
  * COMMIT
  * }}}
  *
  * '''Pattern 2: Event Depends on Result (withEventFactory)'''
  * {{{
  * // Use when event needs data from action result (e.g., generated ID)
  * def createWithEvent(order: Order): DBIO[Long] =
  *   withEventFactory((orders returning orders.map(_.id)) += order) { orderId =>
  *     OrderCreatedEvent(orderId, order.customerId, order.totalAmount, ...)
  *   }
  *
  * // Transaction:
  * BEGIN
  *   INSERT INTO orders (...) VALUES (...) RETURNING id  -- Returns 123
  *   INSERT INTO outbox_events (event_type='OrderCreated', aggregate_id='123')
  * COMMIT
  * }}}
  *
  * == Error Handling ==
  * {{{
  * # Scenario 1: Business action fails
  * withEvent(event) { orders.filter(_.id === 999).map(_.status).update("SHIPPED") }
  * # → No rows updated (order 999 doesn't exist)
  * # → Transaction rolls back automatically
  * # → No event saved (transactionally consistent)
  *
  * # Scenario 2: Database constraint violation
  * withEventFactory(orders += duplicateOrder) { orderId => OrderCreatedEvent(...) }
  * # → INSERT fails (duplicate key)
  * # → Transaction rolls back
  * # → No event saved
  *
  * # Scenario 3: App crashes after COMMIT
  * withEvent(event) { action }
  * # → Transaction committed (both order + event saved)
  * # → App crashes before OutboxProcessor runs
  * # → On restart: OutboxProcessor picks up PENDING event from outbox_events table
  * # → Event published to external systems
  * # → Guaranteed delivery!
  * }}}
  *
  * == Integration with OutboxProcessor ==
  * {{{
  * # 1. Repository saves event (this trait)
  * db.run(createWithEvent(order))
  * # → INSERT INTO outbox_events (status='PENDING', ...)
  *
  * # 2. OutboxProcessor polls for pending events
  * SELECT * FROM outbox_events WHERE status = 'PENDING' ORDER BY created_at LIMIT 100
  * # → Finds event with status=PENDING
  *
  * # 3. OutboxProcessor publishes via HttpEventPublisher
  * # → inventory: POST /api/inventory/reserve
  * # → fraudCheck: POST /api/fraud/check
  * # → shipping: POST /api/domestic-shipping
  * # → billing: POST /api/billing
  *
  * # 4. On success, marks event as PROCESSED
  * UPDATE outbox_events SET status = 'PROCESSED' WHERE id = 456
  * }}}
  *
  * == Idempotency ==
  * {{{
  * # Prevents duplicate event publishing
  * # Idempotency key format: "{aggregateId}:{eventType}"
  * # Example: "123:OrderCreated"
  *
  * # Database constraint:
  * CREATE UNIQUE INDEX ON outbox_events (idempotency_key) WHERE status != 'PROCESSED'
  *
  * # Scenario: Duplicate event attempt
  * withEvent(OrderCreatedEvent(orderId=123, ...)) { action1 }  # First call
  * # → Idempotency key: "123:OrderCreated"
  * # → INSERT succeeds
  *
  * withEvent(OrderCreatedEvent(orderId=123, ...)) { action2 }  # Duplicate call
  * # → Idempotency key: "123:OrderCreated"
  * # → INSERT fails (unique constraint violation)
  * # → Transaction rolls back (action2 also rolled back)
  * # → Prevents duplicate processing
  * }}}
  */
trait OutboxHelper {
  protected val outbox = TableQuery[OutboxTable]

  /** Executes an action and saves an event atomically within the same transaction.
    *
    * Use this when the event can be created upfront (before executing the action).
    * Both the action and event are committed together in a single transaction.
    *
    * == Example: Update Order Status ==
    * {{{
    * def updateStatusWithEvent(orderId: Long, newStatus: String): DBIO[Int] =
    *   for {
    *     orderOpt <- findById(orderId)
    *     order <- orderOpt match {
    *       case Some(o) => DBIO.successful(o)
    *       case None => DBIO.failed(new NoSuchElementException(s"Order $orderId not found"))
    *     }
    *     updated <- withEvent(
    *       OrderStatusUpdatedEvent(
    *         orderId   = orderId,
    *         oldStatus = order.orderStatus,
    *         newStatus = newStatus
    *       )
    *     ) {
    *       orders.filter(_.id === orderId).map(_.orderStatus).update(newStatus)
    *     }
    *   } yield updated
    *
    * # Transaction:
    * BEGIN
    *   UPDATE orders SET order_status = 'SHIPPED' WHERE id = 123
    *   INSERT INTO outbox_events (
    *     aggregate_id = '123',
    *     event_type = 'OrderStatusUpdated',
    *     payloads = '{"notifications": {...}}',
    *     idempotency_key = '123:OrderStatusUpdated',
    *     status = 'PENDING'
    *   )
    * COMMIT
    * }}}
    *
    * == Transaction Semantics ==
    * - If action fails → transaction rolls back, no event saved
    * - If event save fails → transaction rolls back, action reverted
    * - If both succeed → both committed atomically
    *
    * @param event The domain event to save (must implement DomainEvent trait)
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

  /** Executes an action and creates an event from the action's result, saving both atomically.
    *
    * Use this when the event needs data from the action's result (e.g., auto-generated ID).
    * The eventFactory function receives the action result and creates the event.
    *
    * == Example: Create Order with Generated ID ==
    * {{{
    * def createWithEvent(order: Order): DBIO[Long] =
    *   withEventFactory((orders returning orders.map(_.id)) += order) { orderId =>
    *     OrderCreatedEvent(
    *       orderId      = orderId,  // Uses generated ID from action result
    *       customerId   = order.customerId,
    *       totalAmount  = order.totalAmount,
    *       shippingType = order.shippingType
    *     )
    *   }
    *
    * # Transaction:
    * BEGIN
    *   INSERT INTO orders (customer_id, total_amount, shipping_type, order_status)
    *   VALUES ('customer-123', 99.99, 'domestic', 'PENDING')
    *   RETURNING id  -- Returns 123
    *
    *   INSERT INTO outbox_events (
    *     aggregate_id = '123',  -- Generated ID passed to eventFactory
    *     event_type = 'OrderCreated',
    *     payloads = '{
    *       "inventory": {"orderId": 123, "totalAmount": 99.99, "shippingType": "domestic"},
    *       "fraudCheck": {...},
    *       "shipping": {"customerId": "customer-123", "shippingType": "domestic"},
    *       "billing": {"amount": 99.99, "currency": "USD"}
    *     }',
    *     idempotency_key = '123:OrderCreated',
    *     status = 'PENDING'
    *   )
    * COMMIT
    * }}}
    *
    * == Common Use Cases ==
    * - Creating entities with auto-generated IDs
    * - Events that need to reference the created entity's ID
    * - Aggregating multiple action results into event payload
    *
    * @param action The database action to execute (often an INSERT with RETURNING)
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

  /** Saves a domain event to the outbox_events table with idempotency key.
    *
    * Converts DomainEvent to OutboxEvent and persists it. The idempotency key
    * ensures duplicate events are rejected by the database unique constraint.
    *
    * == Event Conversion ==
    * {{{
    * # Domain Event (business model)
    * OrderCreatedEvent(orderId=123, customerId="C-456", totalAmount=99.99)
    *
    * # Converted to OutboxEvent (persistence model)
    * OutboxEvent(
    *   aggregateId = "123",
    *   eventType = "OrderCreated",
    *   payloads = Map(
    *     "inventory" -> DestinationConfig(payload=Some({...}), pathParams=Map(...)),
    *     "fraudCheck" -> DestinationConfig(...),
    *     "shipping" -> DestinationConfig(...),
    *     "billing" -> DestinationConfig(...)
    *   ),
    *   status = PENDING,
    *   idempotencyKey = "123:OrderCreated"  # Ensures uniqueness
    * )
    * }}}
    *
    * @param event The domain event to save
    * @return DBIO action that returns the generated outbox event ID
    */
  private def saveEvent(event: DomainEvent): DBIO[Long] =
    (outbox returning outbox.map(_.id)) += OutboxEvent(
      aggregateId = event.aggregateId,
      eventType   = event.eventType,
      payloads    = event.toPayloads // Destination-specific payloads from domain event
    ).withIdempotencyKey
}
