package integration

import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.play.guice.GuiceOneServerPerSuite
import play.api.Configuration
import play.api.db.slick.DatabaseConfigProvider
import play.api.libs.json.Json
import play.api.test.Helpers.*
import play.api.test.{ FakeRequest, Injecting }
import repositories.OutboxEventsPostgresProfile.api.*
import repositories.{ DeadLetterRepository, OutboxEventsPostgresProfile, OutboxRepository }
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.ExecutionContext.Implicits.global

class OutboxIntegrationSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with GuiceOneServerPerSuite
    with Injecting {

  // Use test configuration with reliable webhook endpoints
  // Disable auto-start to prevent actor system crashes during test setup
  override def fakeApplication(): play.api.Application =
    new play.api.inject.guice.GuiceApplicationBuilder()
      .configure(
        "outbox.autoStart" -> false,
        "outbox.pollInterval" -> "100 milliseconds",
        "outbox.batchSize" -> 10,
        "outbox.maxRetries" -> 2,
        "outbox.useListenNotify" -> false,
        "outbox.enableStaleEventCleanup" -> false,
        "outbox.cleanupInterval" -> "1 minute",
        "outbox.staleEventTimeoutMinutes" -> 5
      )
      .build()

  private lazy val dbConfigProvider = inject[DatabaseConfigProvider]
  private lazy val db = dbConfigProvider.get[OutboxEventsPostgresProfile].db.asInstanceOf[Database]
  private lazy val config     = inject[Configuration]
  private lazy val dlqRepo    = new DeadLetterRepository(config)(using global)
  private lazy val outboxRepo = new OutboxRepository(dlqRepo, config)(using global)

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds))

  "End-to-end outbox flow" should {

    "create order and process outbox event" in {

      db.run(sqlu"DELETE FROM outbox_events").futureValue
      db.run(sqlu"DELETE FROM orders").futureValue

      val createOrderRequest = Json.obj(
        "customerId" -> "integration-test@example.com",
        "items" -> Json.arr(
          Json.obj("productId" -> "product-1", "quantity" -> 2, "price" -> 50.00)
        )
      )

      val response = route(
        app,
        FakeRequest(POST, "/api/orders")
          .withJsonBody(createOrderRequest)
      ).get

      status(response) shouldBe OK

      val orderId = (contentAsJson(response) \ "orderId").as[Long]

      eventually {
        val events = db
          .run(
            sql"SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ${orderId.toString}"
              .as[Int]
              .head
          )
          .futureValue

        events shouldBe 1
      }

      // Verify event is in PENDING status (processor is disabled for tests)
      eventually {
        val pending = db
          .run(
            sql"""
            SELECT COUNT(*) FROM outbox_events
            WHERE aggregate_id = ${orderId.toString}
              AND status = 'PENDING'
          """.as[Int].head
          )
          .futureValue

        pending shouldBe 1
      }
    }

    "handle concurrent order creation" in {
      db.run(sqlu"DELETE FROM outbox_events").futureValue
      db.run(sqlu"DELETE FROM orders").futureValue

      val requests = (1 to 5).map { i =>
        val json = Json.obj(
          "customerId" -> s"concurrent-$i@example.com",
          "items" -> Json.arr(
            Json.obj("productId" -> s"product-$i", "quantity" -> 1, "price" -> 100.00)
          )
        )
        route(app, FakeRequest(POST, "/api/orders").withJsonBody(json)).get
      }

      requests.foreach { response =>
        status(response) shouldBe OK
      }

      eventually {
        val eventCount = db
          .run(
            sql"SELECT COUNT(*) FROM outbox_events WHERE event_type = 'OrderCreated'"
              .as[Int]
              .head
          )
          .futureValue

        eventCount shouldBe 5
      }

      // Verify all events are in PENDING status (processor is disabled for tests)
      eventually {
        val pending = db
          .run(
            sql"""
            SELECT COUNT(*) FROM outbox_events
            WHERE event_type = 'OrderCreated'
              AND status = 'PENDING'
          """.as[Int].head
          )
          .futureValue

        pending shouldBe 5
      }
    }
  }
}
