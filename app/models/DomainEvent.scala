package models

import play.api.libs.json.{ JsValue, Json }

import java.time.Instant

sealed trait DomainEvent {
  def aggregateId: String
  def eventType: String
  def toJson: JsValue
}

case class OrderCreatedEvent(
    orderId: Long,
    customerId: String,
    totalAmount: BigDecimal,
    timestamp: Instant = Instant.now()
) extends DomainEvent {
  override def aggregateId: String = orderId.toString

  override def eventType: String = "OrderCreated"

  override def toJson: JsValue = Json.obj(
    "orderId" -> orderId,
    "customerId" -> customerId,
    "totalAmount" -> totalAmount,
    "timestamp" -> timestamp.toString
  )
}

case class OrderStatusUpdatedEvent(
    orderId: Long,
    oldStatus: String,
    newStatus: String,
    timestamp: Instant = Instant.now()
) extends DomainEvent {
  override def aggregateId: String = orderId.toString
  override def eventType: String   = "OrderStatusUpdated"

  override def toJson: JsValue = Json.obj(
    "orderId" -> orderId,
    "oldStatus" -> oldStatus,
    "newStatus" -> newStatus,
    "timestamp" -> timestamp.toString
  )
}

case class OrderCancelledEvent(
    orderId: Long,
    reason: String,
    timestamp: Instant = Instant.now()
) extends DomainEvent {
  override def aggregateId: String = orderId.toString
  override def eventType: String   = "OrderCancelled"

  override def toJson: JsValue = Json.obj(
    "orderId" -> orderId,
    "reason" -> reason,
    "timestamp" -> timestamp.toString
  )
}
