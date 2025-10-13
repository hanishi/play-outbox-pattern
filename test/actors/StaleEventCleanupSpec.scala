package actors

import actors.OutboxProcessor.Stop
import models.{ DestinationConfig, OutboxEvent }
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerTest
import play.api.Configuration
import play.api.db.slick.DatabaseConfigProvider
import play.api.libs.json.Json
import publishers.{ EventRouter, HttpEndpointConfig, StaleEventPublisher }
import repositories.OutboxEventsPostgresProfile.api.*
import repositories.{
  DeadLetterRepository,
  DestinationResultRepository,
  OutboxEventsPostgresProfile,
  OutboxRepository
}
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.*

class StaleEventCleanupSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with GuiceOneAppPerTest
    with BeforeAndAfterAll {

  private val testKit = ActorTestKit()
  // Mock EventRouter that returns empty endpoints (tests don't need actual routing)
  private val mockEventRouter = new EventRouter {
    override def routeEvent(event: OutboxEvent): HttpEndpointConfig =
      HttpEndpointConfig(
        url             = Some("http://localhost"),
        method          = "POST",
        headers         = Map.empty,
        timeout         = 5.seconds,
        destinationName = "mock-destination",
        revert          = None
      )
    override def routeEventToMultiple(event: OutboxEvent): List[HttpEndpointConfig] = List.empty
  }

  // Ensure test configuration is used
  override def fakeApplication(): play.api.Application =
    new play.api.inject.guice.GuiceApplicationBuilder()
      .loadConfig(play.api.Configuration.load(play.api.Environment.simple()))
      .configure(
        "outbox.useListenNotify" -> false,
        "outbox.enableStaleEventCleanup" -> false
      )
      .build()

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds))

  override def afterAll(): Unit =
    testKit.shutdownTestKit()

  private def getDb: Database = {
    val dbConfigProvider = app.injector.instanceOf[DatabaseConfigProvider]
    dbConfigProvider.get[OutboxEventsPostgresProfile].db.asInstanceOf[Database]
  }

  private def getConfig: Configuration = app.injector.instanceOf[Configuration]

  private def cleanDatabase(db: Database): Unit = {
    db.run(sqlu"DELETE FROM outbox_events").futureValue
    db.run(sqlu"DELETE FROM dead_letter_events").futureValue
  }

  "Stale event cleanup" should {

    "detect and reset events stuck in PROCESSING" in {
      given db: Database = getDb
      val config         = getConfig
      val dlqRepo        = new DeadLetterRepository(config)(using global)
      val resultRepo     = new DestinationResultRepository()(using global)
      val outboxRepo     = new OutboxRepository(dlqRepo, config)(using global)

      cleanDatabase(db)

      val stalePublisher = new StaleEventPublisher

      val processor = testKit.spawn(
        OutboxProcessor(
          publisher           = stalePublisher,
          outboxRepo          = outboxRepo,
          dlqRepo             = dlqRepo,
          resultRepo          = resultRepo,
          eventRouter         = mockEventRouter,
          pollInterval        = 500.millis,
          batchSize           = 10,
          maxRetries          = 2,
          useListenNotify     = false,
          staleCleanupEnabled = true,
          staleTimeoutMinutes = 0, // Set to 0 so events are immediately stale
          cleanupInterval     = 500.millis // Check frequently for test speed
        )
      )

      val outboxEvent = OutboxEvent(
        aggregateId = "1",
        eventType   = "OrderCreated",
        payloads    = Map(
          "OrderCreated" -> DestinationConfig(
            payload = Some(Json.parse(
              """{"orderId":1,"customerId":"test@example.com","totalAmount":100}"""
            ))
          )
        )
      ).withIdempotencyKey
      val eventId = db.run(outboxRepo.insert(outboxEvent)).futureValue

      db.run(
        sqlu"UPDATE outbox_events SET status = 'PROCESSING', status_changed_at = NOW() WHERE id = $eventId"
      ).futureValue

      val initialStatus = db
        .run(
          sql"SELECT status FROM outbox_events WHERE id = $eventId".as[String].head
        )
        .futureValue
      initialStatus shouldBe "PROCESSING"

      eventually {
        val status = db
          .run(
            sql"SELECT status FROM outbox_events WHERE id = $eventId".as[String].head
          )
          .futureValue
        status should (be("PENDING") or be("PROCESSED"))
      }

      eventually {
        val processed = db
          .run(
            sql"SELECT status FROM outbox_events WHERE id = $eventId".as[String].head
          )
          .futureValue
        processed shouldBe "PROCESSED"
      }

      val probe = testKit.createTestProbe[OutboxProcessor.Stopped.type]()
      processor ! Stop(probe.ref)
      probe.expectMessage(OutboxProcessor.Stopped)

      testKit.stop(processor)
      Thread.sleep(1000)
    }

    "only reset events that are actually stale" in {
      given db: Database = getDb
      val config         = getConfig
      val dlqRepo        = new DeadLetterRepository(config)(using global)
      val resultRepo     = new DestinationResultRepository()(using global)
      val outboxRepo     = new OutboxRepository(dlqRepo, config)(using global)

      cleanDatabase(db)

      val stalePublisher = new StaleEventPublisher

      val processor = testKit.spawn(
        OutboxProcessor(
          publisher           = stalePublisher,
          outboxRepo          = outboxRepo,
          dlqRepo             = dlqRepo,
          resultRepo          = resultRepo,
          eventRouter         = mockEventRouter,
          pollInterval        = 500.millis,
          batchSize           = 10,
          maxRetries          = 2,
          useListenNotify     = false,
          staleCleanupEnabled = true,
          staleTimeoutMinutes = 5, // 5 minutes timeout
          cleanupInterval     = 500.millis
        )
      )

      val outboxEvent = OutboxEvent(
        aggregateId = "1",
        eventType   = "OrderCreated",
        payloads    = Map(
          "OrderCreated" -> DestinationConfig(
            payload = Some(Json.parse(
              """{"orderId":1,"customerId":"test@example.com","totalAmount":100}"""
            ))
          )
        )
      ).withIdempotencyKey
      val eventId = db.run(outboxRepo.insert(outboxEvent)).futureValue

      db.run(
        sqlu"UPDATE outbox_events SET status = 'PROCESSING', status_changed_at = NOW() WHERE id = $eventId"
      ).futureValue

      val initialStatus = db
        .run(
          sql"SELECT status FROM outbox_events WHERE id = $eventId".as[String].head
        )
        .futureValue
      initialStatus shouldBe "PROCESSING"

      Thread.sleep(2000)

      val status = db
        .run(
          sql"SELECT status FROM outbox_events WHERE id = $eventId".as[String].head
        )
        .futureValue

      status shouldBe "PROCESSING"

      val probe = testKit.createTestProbe[OutboxProcessor.Stopped.type]()
      processor ! Stop(probe.ref)
      probe.expectMessage(OutboxProcessor.Stopped)
    }
  }
}
