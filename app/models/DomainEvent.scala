package models

import play.api.libs.json.{ JsValue, Json }

import java.time.Instant

sealed trait DomainEvent {
  def aggregateId: String
  def eventType: String

  /** Returns destination-specific configurations (payload + path params) for fan-out publishing.
    * Default implementation creates a single config with payload only, mapped to the event type.
    * Override this method to provide different payloads and path params for different destinations.
    *
    * Example:
    *   Map(
    *     "inventory" -> DestinationConfig(
    *       payload = inventoryJson,
    *       pathParams = Map("orderId" -> "123")
    *     )
    *   )
    */
  def toPayloads: Map[String, DestinationConfig] =
    Map(eventType -> DestinationConfig(payload = Some(toPayload)))

  /** Returns the canonical JSON representation of this event.
    * Used as the default payload when destination-specific payloads are not provided.
    */
  def toPayload: JsValue
}

case class OrderCreatedEvent(
    orderId: Long,
    customerId: String,
    totalAmount: BigDecimal,
    shippingType: String,
    timestamp: Instant = Instant.now()
) extends DomainEvent {
  override def aggregateId: String = orderId.toString

  override def eventType: String = "OrderCreated"

  override def toPayloads: Map[String, DestinationConfig] = Map(

    "OrderCreated" -> DestinationConfig(
      payload    = Some(toPayload),
      pathParams = Map("orderId" -> orderId.toString)
    ),

    "inventory" -> DestinationConfig(
      payload = Some(Json.obj(
        "totalAmount" -> totalAmount,
        "shippingType" -> shippingType,
        "timestamp" -> timestamp.toString
      )),
      pathParams = Map("orderId" -> orderId.toString)
    ),

    "shipping" -> DestinationConfig(
      payload = Some(Json.obj(
        "customerId" -> customerId,
        "totalAmount" -> totalAmount,
        "shippingType" -> shippingType,
        "timestamp" -> timestamp.toString
      )),
      pathParams = Map(
        "orderId" -> orderId.toString,
        "customerId" -> customerId
      )
    ),

    "billing" -> DestinationConfig(
      payload = Some(Json.obj(
        "amount" -> totalAmount,
        "currency" -> "USD",
        "timestamp" -> timestamp.toString
      )),
      pathParams = Map(
        "orderId" -> orderId.toString,
        "customerId" -> customerId
      )
    ),

    "email" -> DestinationConfig(
      payload    = Some(toPayload),
      pathParams = Map("orderId" -> orderId.toString)
    )
  )

  override def toPayload: JsValue = Json.obj(
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

  override def toPayload: JsValue = Json.obj(
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

  override def toPayload: JsValue = Json.obj(
    "orderId" -> orderId,
    "reason" -> reason,
    "timestamp" -> timestamp.toString
  )
}
