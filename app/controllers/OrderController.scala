package controllers

import models.Order
import play.api.libs.json.*
import play.api.mvc.*
import repositories.{ DeadLetterRepository, OutboxRepository }
import services.OrderService
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api.*

import java.time.Instant
import javax.inject.*
import scala.concurrent.ExecutionContext

@Singleton
class OrderController @Inject() (
    cc: ControllerComponents,
    orderService: OrderService,
    outboxRepo: OutboxRepository,
    dlqRepo: DeadLetterRepository,
    db: Database
)(using ec: ExecutionContext)
    extends AbstractController(cc) {

  def createOrder: Action[AnyContent] = Action.async { request =>
    val customerId = request.body.asFormUrlEncoded
      .flatMap(_.get("customerId").flatMap(_.headOption))
      .getOrElse("customer-123")
    val totalAmount = request.body.asFormUrlEncoded
      .flatMap(_.get("totalAmount").flatMap(_.headOption))
      .flatMap(s => scala.util.Try(BigDecimal(s)).toOption)
      .getOrElse(BigDecimal(99.99))
    val shippingType = request.body.asFormUrlEncoded
      .flatMap(_.get("shippingType").flatMap(_.headOption))
      .getOrElse("domestic")

    val order = Order(
      customerId   = customerId,
      totalAmount  = totalAmount,
      shippingType = shippingType,
      orderStatus  = "PENDING",
      createdAt    = Instant.now(),
      updatedAt    = Instant.now()
    )

    orderService
      .createOrder(order)
      .map { orderId =>
        Ok(
          s"""<div class="success">Order #$orderId created successfully! Event queued in outbox.</div>"""
        )
          .as("text/html")
      }
      .recover { case ex =>
        InternalServerError(s"""<div class="error">Error: ${ex.getMessage}</div>""")
          .as("text/html")
      }
  }

  def updateStatus(id: Long): Action[AnyContent] = Action.async { request =>
    val status = request.body.asFormUrlEncoded
      .flatMap(_.get("status").flatMap(_.headOption))
      .getOrElse("PROCESSING")

    orderService
      .updateOrderStatus(id, status)
      .flatMap { _ =>
        listOrders(request)
      }
      .recover {
        case _: NoSuchElementException =>
          Ok(s"""<div class="error">Order $id not found</div>""").as("text/html")
        case ex =>
          Ok(s"""<div class="error">Error: ${ex.getMessage}</div>""").as("text/html")
      }
  }

  def cancelOrder(id: Long): Action[AnyContent] = Action.async { request =>
    orderService
      .cancelOrder(id, "User requested via UI")
      .flatMap { _ =>
        listOrders.apply(request)
      }
      .recover {
        case _: NoSuchElementException =>
          Ok(s"""<div class="error">Order $id not found</div>""").as("text/html")
        case ex =>
          Ok(s"""<div class="error">Error: ${ex.getMessage}</div>""").as("text/html")
      }
  }

  def listOrders: Action[AnyContent] = Action.async {
    orderService
      .listOrdersWithResults(10, 0)
      .map { ordersWithResults =>
        if (ordersWithResults.isEmpty) {
          Ok(
            """<p style="color: #718096; text-align: center; padding: 2rem;">No orders yet. Create one to get started!</p>"""
          )
            .as("text/html")
        } else {
          val html = ordersWithResults
            .map { case models.OrderWithResults(order, results) =>
              // Check if all non-REVERT destinations succeeded (needed to enable Ship/Cancel buttons)
              val publishResults = results.filter(!_.eventType.endsWith(":REVERT"))
              val allSucceeded   = publishResults.nonEmpty && publishResults.forall(_.success)

              // Check if revert operations have been executed (compensation completed)
              val hasRevertEvents = results.exists(_.eventType.endsWith(":REVERT"))

              val isFinal     = order.orderStatus == "SHIPPED" || order.orderStatus == "CANCELLED"
              val canRemove   = isFinal || hasRevertEvents
              val buttonsHtml =
                if (canRemove) s"""
            <div class="order-actions">
              <button class="btn-danger" hx-delete="/orders/${order.id}/delete" hx-target="#orders" hx-swap="innerHTML">Remove</button>
            </div>
            """
                else if (allSucceeded)
                  s"""
            <div class="order-actions">
              <button class="btn-secondary" hx-put="/orders/${order.id}/status" hx-vals='{"status":"SHIPPED"}' hx-target="#orders" hx-swap="innerHTML">Ship</button>
              <button class="btn-danger" hx-delete="/orders/${order.id}/cancel" hx-target="#orders" hx-swap="innerHTML">Cancel</button>
            </div>
            """
                else
                  s"""
            <div class="order-actions">
              <div style="font-size: 0.75rem; color: #EF4444; font-style: italic;">Publishing failed - actions disabled</div>
            </div>
            """

              val resultsHtml = if (results.nonEmpty) {
                val resultsDetails = results
                  .sortBy(r =>
                    (!r.eventType.endsWith(":REVERT"), r.destination)
                  ) // Show original attempts first, then reverts
                  .map { result =>
                    val isRevert    = result.eventType.endsWith(":REVERT")
                    val statusColor = if (result.success) "#10B981" else "#EF4444"
                    val statusIcon  = if (result.success) "✓" else "✗"
                    val statusCode  = result.responseStatus.map(c => s" ($c)").getOrElse("")
                    val eventLabel  =
                      if (isRevert)
                        s"<span style=\"color: #9333EA; font-weight: 600;\">[REVERT]</span> "
                      else ""
                    val errorMsg = result.errorMessage
                      .map(msg =>
                        s"<div style=\"font-size: 0.75rem; color: #EF4444; margin-top: 0.25rem;\">${msg
                            .take(50)}${if (msg.length > 50) "..." else ""}</div>"
                      )
                      .getOrElse("")
                    s"""
                  <div style="display: flex; align-items: center; gap: 0.5rem; padding: 0.25rem 0;">
                    <span style="color: $statusColor; font-weight: bold;">$statusIcon</span>
                    <span style="font-size: 0.813rem;">$eventLabel${result.destination}$statusCode</span>
                    $errorMsg
                  </div>
                """
                  }
                  .mkString("\n")

                s"""
            <div style="margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid #E5E7EB;">
              <div style="font-size: 0.75rem; color: #6B7280; margin-bottom: 0.5rem;">
                Event: ${results.headOption
                    .map(_.eventType.replace(":REVERT", ""))
                    .getOrElse("Unknown")}
              </div>
              $resultsDetails
            </div>
            """
              } else ""

              s"""
          <div class="order-item">
            <div class="order-header">
              <span class="order-id">Order #${order.id}</span>
              <span class="order-status status-${order.orderStatus.toLowerCase}">${order.orderStatus}</span>
            </div>
            <div style="color: #718096; font-size: 0.875rem;">
              <div>Customer: ${order.customerId}</div>
              <div>Amount: $$${order.totalAmount}</div>
              <div>Shipping: ${order.shippingType.capitalize}</div>
            </div>
            $resultsHtml
            $buttonsHtml
          </div>
          """
            }
            .mkString("\n")

          Ok(html).as("text/html")
        }
      }
      .recover { case ex =>
        Ok(s"""<div class="error">Error loading orders: ${ex.getMessage}</div>""").as("text/html")
      }
  }

  def deleteOrder(id: Long): Action[AnyContent] = Action.async { request =>
    orderService
      .deleteOrder(id)
      .flatMap { _ =>
        listOrders.apply(request)
      }
      .recover { case ex =>
        Ok(s"""<div class="error">Error: ${ex.getMessage}</div>""").as("text/html")
      }
  }

  def outboxStats: Action[AnyContent] = Action.async {
    for {
      pending <- db.run(outboxRepo.countPending)
      processing <- db.run(
        sql"SELECT COUNT(*) FROM outbox_events WHERE status = 'PROCESSING'".as[Int].head
      )
      successful <- db.run(outboxRepo.countSuccessfullyProcessed)
      dlqTotal <- db.run(dlqRepo.countAll)
      dlqMaxRetries <- db.run(dlqRepo.countByReason("MAX_RETRIES_EXCEEDED"))
      dlqNonRetryable <- db.run(dlqRepo.countByReason("NON_RETRYABLE_ERROR"))
    } yield {
      val totalPendingAndProcessing = pending + processing
      val html                      = s"""
      <div class="stat-grid">
        <div class="stat">
          <div class="stat-value">$totalPendingAndProcessing</div>
          <div class="stat-label">Queued Events</div>
          ${
          if (processing > 0)
            s"""<div style="font-size: 0.75rem; color: #4299e1; margin-top: 0.25rem;">Processing: $processing</div>"""
          else ""
        }
        </div>
        <div class="stat" style="background: ${
          if (successful > 0) "#F0FDF4" else "white"
        }; border-color: ${if (successful > 0) "#86EFAC" else "#E5E7EB"};">
          <div class="stat-value" style="color: ${
          if (successful > 0) "#16A34A" else "#1F2937"
        };">$successful</div>
          <div class="stat-label">Successfully Published</div>
        </div>
        <div class="stat" style="background: ${
          if (dlqTotal > 0) "#FEF2F2" else "white"
        }; border-color: ${if (dlqTotal > 0) "#FCA5A5" else "#E5E7EB"};">
          <div class="stat-value" style="color: ${
          if (dlqTotal > 0) "#DC2626" else "#1F2937"
        };">$dlqTotal</div>
          <div class="stat-label">Dead Letter Queue</div>
          ${
          if (dlqTotal > 0)
            s"""<div style="font-size: 0.75rem; color: #991B1B; margin-top: 0.25rem;">Max Retries: $dlqMaxRetries | Non-Retryable: $dlqNonRetryable</div>"""
          else ""
        }
        </div>
      </div>
      """
      Ok(html).as("text/html")
    }
  }

  // JSON API endpoints

  def createOrderJson: Action[JsValue] = Action.async(parse.json) { request =>
    val customerId   = (request.body \ "customerId").asOpt[String].getOrElse("customer-123")
    val totalAmount  = (request.body \ "totalAmount").asOpt[BigDecimal].getOrElse(BigDecimal(99.99))
    val shippingType = (request.body \ "shippingType").asOpt[String].getOrElse("domestic")

    val order = Order(
      customerId   = customerId,
      totalAmount  = totalAmount,
      shippingType = shippingType,
      orderStatus  = "PENDING",
      createdAt    = Instant.now(),
      updatedAt    = Instant.now()
    )

    orderService
      .createOrder(order)
      .map { orderId =>
        Ok(
          Json.obj(
            "success" -> true,
            "orderId" -> orderId,
            "message" -> s"Order #$orderId created successfully! Event queued in outbox."
          )
        )
      }
      .recover { case ex =>
        InternalServerError(
          Json.obj(
            "success" -> false,
            "error" -> ex.getMessage
          )
        )
      }
  }

  def updateStatusJson(id: Long): Action[JsValue] = Action.async(parse.json) { request =>
    val status = (request.body \ "status").asOpt[String].getOrElse("PROCESSING")

    orderService
      .updateOrderStatus(id, status)
      .map { _ =>
        Ok(
          Json.obj(
            "success" -> true,
            "orderId" -> id,
            "status" -> status,
            "message" -> s"Order #$id status updated to $status"
          )
        )
      }
      .recover {
        case _: NoSuchElementException =>
          NotFound(
            Json.obj(
              "success" -> false,
              "error" -> s"Order $id not found"
            )
          )
        case ex =>
          InternalServerError(
            Json.obj(
              "success" -> false,
              "error" -> ex.getMessage
            )
          )
      }
  }

  def cancelOrderJson(id: Long): Action[AnyContent] = Action.async {
    orderService
      .cancelOrder(id, "User requested via API")
      .map { _ =>
        Ok(
          Json.obj(
            "success" -> true,
            "orderId" -> id,
            "message" -> s"Order #$id cancelled successfully"
          )
        )
      }
      .recover {
        case _: NoSuchElementException =>
          NotFound(
            Json.obj(
              "success" -> false,
              "error" -> s"Order $id not found"
            )
          )
        case ex =>
          InternalServerError(
            Json.obj(
              "success" -> false,
              "error" -> ex.getMessage
            )
          )
      }
  }

  def deleteOrderJson(id: Long): Action[AnyContent] = Action.async {
    orderService
      .deleteOrder(id)
      .map { _ =>
        Ok(
          Json.obj(
            "success" -> true,
            "orderId" -> id,
            "message" -> s"Order #$id deleted successfully"
          )
        )
      }
      .recover { case ex =>
        InternalServerError(
          Json.obj(
            "success" -> false,
            "error" -> ex.getMessage
          )
        )
      }
  }

  def listOrdersJson: Action[AnyContent] = Action.async {
    orderService
      .listOrders(10, 0)
      .map { orders =>
        Ok(
          Json.obj(
            "success" -> true,
            "orders" -> Json.toJson(orders)
          )
        )
      }
      .recover { case ex =>
        InternalServerError(
          Json.obj(
            "success" -> false,
            "error" -> ex.getMessage
          )
        )
      }
  }

  def outboxStatsJson: Action[AnyContent] = Action.async {
    for {
      pending <- db.run(outboxRepo.countPending)
      successful <- db.run(outboxRepo.countSuccessfullyProcessed)
      dlqTotal <- db.run(dlqRepo.countAll)
      dlqMaxRetries <- db.run(dlqRepo.countByReason("MAX_RETRIES_EXCEEDED"))
      dlqNonRetryable <- db.run(dlqRepo.countByReason("NON_RETRYABLE_ERROR"))
    } yield {
      Ok(
        Json.obj(
          "success" -> true,
          "stats" -> Json.obj(
            "pendingEvents" -> pending,
            "successfullyPublished" -> successful,
            "deadLetterQueue" -> Json.obj(
              "total" -> dlqTotal,
              "maxRetries" -> dlqMaxRetries,
              "nonRetryable" -> dlqNonRetryable
            )
          )
        )
      )
    }
  }

  def debugEventJson(id: Long): Action[AnyContent] = Action.async {
    db.run(outboxRepo.find(id))
      .map { event =>
        val now      = java.time.Instant.now()
        val readyNow = event.nextRetryAt.forall(!_.isAfter(now))
        Ok(
          Json.obj(
            "success" -> true,
            "event" -> Json.toJson(event),
            "debug" -> Json.obj(
              "currentTime" -> now.toString,
              "readyForRetry" -> readyNow,
              "secondsUntilRetry" -> event.nextRetryAt.map { next =>
                (next.toEpochMilli - now.toEpochMilli) / 1000.0
              }
            )
          )
        )
      }
      .recover {
        case ex: NoSuchElementException =>
          NotFound(Json.obj("success" -> false, "error" -> s"Event $id not found"))
        case ex =>
          InternalServerError(Json.obj("success" -> false, "error" -> ex.getMessage))
      }
  }

  def listAllEventsJson: Action[AnyContent] = Action.async {
    import slick.jdbc.PostgresProfile.api.*

    val query = sql"""
      SELECT id, aggregate_id, event_type, status, retry_count,
             created_at, next_retry_at, moved_to_dlq, last_error
      FROM outbox_events
      ORDER BY id DESC
      LIMIT 20
    """.as[
      (
          Long,
          String,
          String,
          String,
          Int,
          java.sql.Timestamp,
          Option[java.sql.Timestamp],
          Boolean,
          Option[String]
      )
    ]

    db.run(query)
      .map { rows =>
        val events = rows.map {
          case (id, aggId, evType, status, retryCount, createdAt, nextRetryAt, dlq, error) =>
            Json.obj(
              "id" -> id,
              "aggregateId" -> aggId,
              "eventType" -> evType,
              "status" -> status,
              "retryCount" -> retryCount,
              "createdAt" -> createdAt.toInstant.toString,
              "nextRetryAt" -> nextRetryAt.map(_.toInstant.toString),
              "movedToDlq" -> dlq,
              "lastError" -> error.map(_.take(100))
            )
        }
        Ok(Json.obj("success" -> true, "events" -> events, "count" -> rows.length))
      }
      .recover { case ex =>
        InternalServerError(Json.obj("success" -> false, "error" -> ex.getMessage))
      }
  }
}
