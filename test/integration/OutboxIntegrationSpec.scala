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
import repositories.{ DeadLetterRepository, OutboxRepository }
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile
import slick.jdbc.PostgresProfile.api.*

import scala.concurrent.ExecutionContext.Implicits.global

class OutboxIntegrationSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with GuiceOneServerPerSuite
    with Injecting {

  private lazy val dbConfigProvider = inject[DatabaseConfigProvider]
  private lazy val db         = dbConfigProvider.get[PostgresProfile].db.asInstanceOf[Database]
  private lazy val config     = inject[Configuration]
  private lazy val dlqRepo    = new DeadLetterRepository()(using global)
  private lazy val outboxRepo = new OutboxRepository(dlqRepo, config)(using global)

  override implicit val patienceConfig: PatienceConfig =
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

      eventually {
        val processed = db
          .run(
            sql"""
            SELECT COUNT(*) FROM outbox_events
            WHERE aggregate_id = ${orderId.toString}
              AND (status = 'PROCESSED' OR retry_count > 0)
          """.as[Int].head
          )
          .futureValue

        processed shouldBe 1
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

      eventually {
        val processedOrRetried = db
          .run(
            sql"""
            SELECT COUNT(*) FROM outbox_events
            WHERE event_type = 'OrderCreated'
              AND (status = 'PROCESSED' OR retry_count > 0)
          """.as[Int].head
          )
          .futureValue

        processedOrRetried shouldBe 5
      }
    }
  }
}
