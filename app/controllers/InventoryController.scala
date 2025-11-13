package controllers

import play.api.{ Configuration, Logging }
import play.api.libs.json.*
import play.api.mvc.*

import javax.inject.*
import scala.util.Random

/** Inventory service controller - handles inventory reservation and release.
  *
  * Endpoints:
  * - POST /api/inventory/reserve - Reserve inventory items
  * - DELETE /api/inventory/:id/release - Release a reservation
  */
@Singleton
class InventoryController @Inject() (cc: ControllerComponents, config: Configuration)(
    using ec: scala.concurrent.ExecutionContext
) extends AbstractController(cc)
    with Logging {

  private val random             = new Random()
  private val reserveFailureRate = config.get[Double]("service.failure.rates.inventory.reserve")
  private val releaseFailureRate = config.get[Double]("service.failure.rates.inventory.release")

  /** Reserve inventory endpoint.
    * Returns a reservationId that can be used for revert.
    * Configurable failure rate from application.conf (service.failure.rates.inventory.reserve).
    */
  def reserve: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[INVENTORY] Reserve request - event=$eventId, aggregate=$aggregateId")
    logger.debug(s"[INVENTORY] Payload: ${request.body}")

    // Simulate inventory system failure based on configured rate
    if (random.nextDouble() < reserveFailureRate) {
      logger.error(
        s"[INVENTORY] Simulating inventory system failure - event=$eventId, aggregate=$aggregateId"
      )
      ServiceUnavailable(
        Json.obj(
          "error" -> "inventory_system_unavailable",
          "message" -> "Inventory management system is temporarily unavailable",
          "aggregateId" -> aggregateId,
          "timestamp" -> System.currentTimeMillis()
        )
      ).withHeaders("Retry-After" -> "4")
    } else {
      // Generate a unique reservation ID
      val reservationId = s"RES-${aggregateId}-${System.currentTimeMillis()}"
      val warehouseId   = "WH-NYC-01"

      // Extract payload details
      val orderId      = (request.body \ "orderId").asOpt[Long].getOrElse(0L)
      val items        = (request.body \ "items").asOpt[JsArray].getOrElse(Json.arr())
      val shippingType = (request.body \ "shippingType").asOpt[String].getOrElse("domestic")

      // Calculate expiry time (15 minutes from now)
      val expiresAt = System.currentTimeMillis() + (15 * 60 * 1000)

      logger.info(
        s"[INVENTORY] Reserved: $reservationId, warehouse: $warehouseId, shippingType: $shippingType"
      )

      Ok(
        Json.obj(
          "reservationId" -> reservationId,
          "orderId" -> orderId,
          "warehouseId" -> warehouseId,
          "warehouse" -> Json.obj(
            "id" -> warehouseId,
            "name" -> "NYC Distribution Center",
            "location" -> Json.obj(
              "city" -> "New York",
              "state" -> "NY",
              "country" -> "US"
            )
          ),
          "items" -> items.as[Seq[JsObject]].map { item =>
            Json.obj(
              "sku" -> (item \ "sku").asOpt[String].getOrElse("UNKNOWN"),
              "quantity" -> (item \ "quantity").asOpt[Int].getOrElse(1),
              "reserved" -> true,
              "binLocation" -> s"A${random.nextInt(20)}-${random.nextInt(50)}"
            )
          },
          "shippingType" -> shippingType,
          "status" -> "reserved",
          "expiresAt" -> expiresAt,
          "reservedAt" -> System.currentTimeMillis()
        )
      )
    }
  }

  /** Reserve inventory endpoint - ALWAYS SUCCEEDS.
    */
  def reserveSucceed: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.info(s"[INVENTORY-SUCCEED] Reserve request - event=$eventId, aggregate=$aggregateId")

    val reservationId = s"RES-${aggregateId}-${System.currentTimeMillis()}"
    val warehouseId   = "WH-NYC-01"
    val orderId       = (request.body \ "orderId").asOpt[Long].getOrElse(0L)
    val items         = (request.body \ "items").asOpt[JsArray].getOrElse(Json.arr())
    val shippingType  = (request.body \ "shippingType").asOpt[String].getOrElse("domestic")
    val expiresAt     = System.currentTimeMillis() + (15 * 60 * 1000)

    logger.info(s"[INVENTORY-SUCCEED] Reserved: $reservationId, warehouse: $warehouseId")

    Ok(
      Json.obj(
        "reservationId" -> reservationId,
        "orderId" -> orderId,
        "warehouseId" -> warehouseId,
        "warehouse" -> Json.obj(
          "id" -> warehouseId,
          "name" -> "NYC Distribution Center",
          "location" -> Json.obj("city" -> "New York", "state" -> "NY", "country" -> "US")
        ),
        "items" -> items.as[Seq[JsObject]].map { item =>
          Json.obj(
            "sku" -> (item \ "sku").asOpt[String].getOrElse("UNKNOWN"),
            "quantity" -> (item \ "quantity").asOpt[Int].getOrElse(1),
            "reserved" -> true,
            "binLocation" -> s"A${random.nextInt(20)}-${random.nextInt(50)}"
          )
        },
        "shippingType" -> shippingType,
        "status" -> "reserved",
        "expiresAt" -> expiresAt,
        "reservedAt" -> System.currentTimeMillis()
      )
    )
  }

  /** Reserve inventory endpoint - ALWAYS FAILS.
    */
  def reserveFail: Action[JsValue] = Action(parse.json) { request =>
    val eventId     = request.headers.get("X-Event-Id").getOrElse("unknown")
    val aggregateId = request.headers.get("X-Aggregate-Id").getOrElse("unknown")

    logger.error(
      s"[INVENTORY-FAIL] Simulating inventory failure - event=$eventId, aggregate=$aggregateId"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "inventory_system_unavailable",
        "message" -> "Inventory management system is temporarily unavailable",
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    ).withHeaders("Retry-After" -> "4")
  }

  /** Release inventory reservation (revert endpoint).
    * URL pattern: /api/inventory/:reservationId/release
    * Configurable failure rate from application.conf (service.failure.rates.inventory.release).
    */
  def release(reservationId: String): Action[AnyContent] = Action { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType   = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination = request.headers.get("X-Revert-Destination").getOrElse("unknown")

    logger.warn(
      s"[INVENTORY-REVERT] Releasing reservation: $reservationId " +
        s"(aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    // Simulate release service failure based on configured rate
    if (random.nextDouble() < releaseFailureRate) {
      logger.error(s"[INVENTORY-REVERT] Simulating release failure for reservation: $reservationId")
      ServiceUnavailable(
        Json.obj(
          "error" -> "inventory_release_unavailable",
          "message" -> "Inventory release service is temporarily unavailable",
          "aggregateId" -> aggregateId,
          "timestamp" -> System.currentTimeMillis()
        )
      ).withHeaders("Retry-After" -> "4")
    } else {
      Ok(
        Json.obj(
          "status" -> "released",
          "message" -> "Inventory reservation released",
          "reservationId" -> reservationId,
          "aggregateId" -> aggregateId,
          "timestamp" -> System.currentTimeMillis()
        )
      )
    }
  }

  /** Release inventory reservation - ALWAYS SUCCEEDS (revert endpoint).
    */
  def releaseSucceed: Action[AnyContent] = Action { request =>
    val aggregateId   = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType     = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination   = request.headers.get("X-Revert-Destination").getOrElse("unknown")
    val reservationId = s"RES-${aggregateId}-TEST"

    logger.warn(
      s"[INVENTORY-REVERT-SUCCEED] Releasing reservation: $reservationId " +
        s"(aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    Ok(
      Json.obj(
        "status" -> "released",
        "message" -> "Inventory reservation released",
        "reservationId" -> reservationId,
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    )
  }

  /** Release inventory reservation - ALWAYS FAILS (revert endpoint).
    */
  def releaseFail: Action[AnyContent] = Action { request =>
    val aggregateId = request.headers.get("X-Revert-Aggregate-Id").getOrElse("unknown")
    val eventType   = request.headers.get("X-Revert-Event-Type").getOrElse("unknown")
    val destination = request.headers.get("X-Revert-Destination").getOrElse("unknown")

    logger.error(
      s"[INVENTORY-REVERT-FAIL] Simulating release failure " +
        s"(aggregate=$aggregateId, eventType=$eventType, destination=$destination)"
    )

    ServiceUnavailable(
      Json.obj(
        "error" -> "inventory_release_unavailable",
        "message" -> "Inventory release service is temporarily unavailable",
        "aggregateId" -> aggregateId,
        "timestamp" -> System.currentTimeMillis()
      )
    ).withHeaders("Retry-After" -> "4")
  }
}
