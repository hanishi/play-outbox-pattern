package models

import play.api.libs.json.*

import java.time.Instant

case class Order(
    id: Long = 0L,
    customerId: String,
    totalAmount: BigDecimal,
    shippingType: String = "domestic",
    orderStatus: String  = "PENDING",
    createdAt: Instant   = Instant.now(),
    updatedAt: Instant   = Instant.now(),
    deleted: Boolean     = false
)

object Order {
  given Format[Order] = Json.format[Order]
}

/** Order with publishing results for UI display */
case class OrderWithResults(order: Order, results: Seq[AggregateResult])

object OrderWithResults {
  given Format[OrderWithResults] = Json.format[OrderWithResults]
}
