package controllers

import play.api.libs.json.*
import play.api.mvc.*

import javax.inject.*
import scala.concurrent.{ ExecutionContext, Future }

/** Simulates a notification service that receives order status updates.
  *
  * In a real system, this would send emails, SMS, push notifications, etc.
  * Demonstrates conditional routing based on order status.
  */
@Singleton
class NotificationController @Inject() (
    val controllerComponents: ControllerComponents
)(using ec: ExecutionContext)
    extends BaseController {

  /** Sends shipping confirmation with tracking information.
    *
    * Triggered when order status changes to "SHIPPED".
    * Would send email/SMS with tracking link in production.
    */
  def shippingConfirmation: Action[JsValue] = Action.async(parse.json) { request =>
    val orderId   = (request.body \ "orderId").as[Long]
    val newStatus = (request.body \ "newStatus").as[String]
    val timestamp = (request.body \ "timestamp").asOpt[String]

    println(s"📧 NOTIFICATION: Order $orderId has been $newStatus")
    println(s"   → Would send tracking email to customer")
    println(s"   → Timestamp: ${timestamp.getOrElse("N/A")}")

    Future.successful(
      Ok(
        Json.obj(
          "status" -> "sent",
          "notificationType" -> "shipping_confirmation",
          "orderId" -> orderId,
          "message" -> s"Shipping confirmation sent for order $orderId",
          "timestamp" -> System.currentTimeMillis()
        )
      )
    )
  }

  /** Sends review request after delivery.
    *
    * Triggered when order status changes to "DELIVERED".
    * Would send email asking customer to review their purchase.
    */
  def reviewRequest: Action[JsValue] = Action.async(parse.json) { request =>
    val orderId   = (request.body \ "orderId").as[Long]
    val newStatus = (request.body \ "newStatus").as[String]

    println(s"⭐ NOTIFICATION: Order $orderId has been $newStatus")
    println(s"   → Would send review request email to customer")
    println(s"   → Including link to review page")

    Future.successful(
      Ok(
        Json.obj(
          "status" -> "sent",
          "notificationType" -> "review_request",
          "orderId" -> orderId,
          "message" -> s"Review request sent for order $orderId",
          "timestamp" -> System.currentTimeMillis()
        )
      )
    )
  }

  /** Generic status update notification.
    *
    * Catches all other status changes for analytics/logging.
    */
  def statusUpdate: Action[JsValue] = Action.async(parse.json) { request =>
    val orderId   = (request.body \ "orderId").as[Long]
    val oldStatus = (request.body \ "oldStatus").as[String]
    val newStatus = (request.body \ "newStatus").as[String]

    println(s"📊 ANALYTICS: Order $orderId status changed: $oldStatus → $newStatus")

    Future.successful(
      Ok(
        Json.obj(
          "status" -> "recorded",
          "orderId" -> orderId,
          "message" -> s"Status update recorded for order $orderId",
          "timestamp" -> System.currentTimeMillis()
        )
      )
    )
  }
}
