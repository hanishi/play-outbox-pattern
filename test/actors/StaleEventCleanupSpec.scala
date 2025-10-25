package actors

import actors.OutboxProcessor.Stop
import models.OutboxEvent
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerTest
import play.api.Configuration
import play.api.db.slick.DatabaseConfigProvider
import publishers.StaleEventPublisher
import repositories.{ DeadLetterRepository, OutboxRepository }
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile
import slick.jdbc.PostgresProfile.api.*

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

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds))

  override def afterAll(): Unit =
    testKit.shutdownTestKit()

  private def getDb: Database = {
    val dbConfigProvider = app.injector.instanceOf[DatabaseConfigProvider]
    dbConfigProvider.get[PostgresProfile].db.asInstanceOf[Database]
  }

  private def getConfig: Configuration = app.injector.instanceOf[Configuration]

  private def cleanDatabase(db: Database): Unit = {
    db.run(sqlu"DELETE FROM outbox_events").futureValue
    db.run(sqlu"DELETE FROM dead_letter_events").futureValue
  }

  "Stale event cleanup" should {

    "detect and reset events stuck in PROCESSING" in {
      val db         = getDb
      val config     = getConfig
      val dlqRepo    = new DeadLetterRepository()(using global)
      val outboxRepo = new OutboxRepository(dlqRepo, config)(using global)

      cleanDatabase(db)

      val stalePublisher = new StaleEventPublisher

      val processor = testKit.spawn(
        OutboxProcessor(
          db                  = db,
          publisher           = stalePublisher,
          outboxRepo          = outboxRepo,
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
        payload     = """{"orderId":1,"customerId":"test@example.com","totalAmount":100}"""
      ).withIdempotencyKey
      val eventId = db.run(outboxRepo.insert(outboxEvent)).futureValue

      db.run(sqlu"UPDATE outbox_events SET status = 'PROCESSING', created_at = NOW() WHERE id = $eventId").futureValue

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
      val db         = getDb
      val config     = getConfig
      val dlqRepo    = new DeadLetterRepository()(using global)
      val outboxRepo = new OutboxRepository(dlqRepo, config)(using global)

      cleanDatabase(db)

      val stalePublisher = new StaleEventPublisher

      val processor = testKit.spawn(
        OutboxProcessor(
          db                  = db,
          publisher           = stalePublisher,
          outboxRepo          = outboxRepo,
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
        payload     = """{"orderId":1,"customerId":"test@example.com","totalAmount":100}"""
      ).withIdempotencyKey
      val eventId = db.run(outboxRepo.insert(outboxEvent)).futureValue

      db.run(sqlu"UPDATE outbox_events SET status = 'PROCESSING', created_at = NOW() WHERE id = $eventId").futureValue

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
