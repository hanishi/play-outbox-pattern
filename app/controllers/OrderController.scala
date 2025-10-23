package controllers

import models.Order
import play.api.mvc.*
import repositories.{ DeadLetterRepository, OutboxRepository }
import services.OrderService
import slick.jdbc.JdbcBackend.Database

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

    val order = Order(
      customerId  = customerId,
      totalAmount = totalAmount,
      orderStatus = "PENDING",
      createdAt   = Instant.now(),
      updatedAt   = Instant.now()
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

  def listOrders: Action[AnyContent] = Action.async {
    orderService
      .listOrders(10, 0)
      .map { orders =>
        if (orders.isEmpty) {
          Ok(
            """<p style="color: #718096; text-align: center; padding: 2rem;">No orders yet. Create one to get started!</p>"""
          )
            .as("text/html")
        } else {
          val html = orders
            .map { order =>
              val isFinal      = order.orderStatus == "SHIPPED" || order.orderStatus == "CANCELLED"
              val disabledAttr = if (isFinal) "disabled" else ""
              val buttonsHtml  =
                if (isFinal) s"""
            <div class="order-actions">
              <button class="btn-danger" hx-delete="/orders/${order.id}/delete" hx-target="#orders" hx-swap="innerHTML">Remove</button>
            </div>
            """
                else
                  s"""
            <div class="order-actions">
              <button class="btn-secondary" hx-put="/orders/${order.id}/status" hx-vals='{"status":"SHIPPED"}' hx-target="#orders" hx-swap="innerHTML">Ship</button>
              <button class="btn-danger" hx-delete="/orders/${order.id}/cancel" hx-target="#orders" hx-swap="innerHTML">Cancel</button>
            </div>
            """

              s"""
          <div class="order-item">
            <div class="order-header">
              <span class="order-id">Order #${order.id}</span>
              <span class="order-status status-${order.orderStatus.toLowerCase}">${order.orderStatus}</span>
            </div>
            <div style="color: #718096; font-size: 0.875rem;">
              <div>Customer: ${order.customerId}</div>
              <div>Amount: $$${order.totalAmount}</div>
            </div>
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

  def outboxStats: Action[AnyContent] = Action.async {
    for {
      pending <- db.run(outboxRepo.countPending)
      successful <- db.run(outboxRepo.countSuccessfullyProcessed)
      dlqTotal <- db.run(dlqRepo.countAll)
      dlqMaxRetries <- db.run(dlqRepo.countByReason("MAX_RETRIES_EXCEEDED"))
      dlqNonRetryable <- db.run(dlqRepo.countByReason("NON_RETRYABLE_ERROR"))
    } yield {
      val html = s"""
      <div class="stat-grid">
        <div class="stat">
          <div class="stat-value">$pending</div>
          <div class="stat-label">Pending Events</div>
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
}
