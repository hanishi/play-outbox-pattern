package actors

import actors.OutboxProcessor.{ ProcessUnhandledEvent, Stop }
import models.{ EventStatus, OutboxEvent }
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerSuite
import play.api.Configuration
import play.api.db.slick.DatabaseConfigProvider
import publishers.{ EventPublisher, PublishResult }
import repositories.{ DeadLetterRepository, OutboxRepository }
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile
import slick.jdbc.PostgresProfile.api.*

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.*

class OutboxProcessorSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with GuiceOneAppPerSuite
    with BeforeAndAfterAll {
  private lazy val dbConfigProvider = app.injector.instanceOf[DatabaseConfigProvider]
  private lazy val db         = dbConfigProvider.get[PostgresProfile].db.asInstanceOf[Database]
  private lazy val config     = app.injector.instanceOf[Configuration]
  private lazy val dlqRepo    = new DeadLetterRepository()(using global)
  private lazy val outboxRepo = new OutboxRepository(dlqRepo, config)(using global)
  private val testKit               = ActorTestKit()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 10.seconds)

  override def afterAll(): Unit =
    testKit.shutdownTestKit()

  private def cleanDatabase(): Unit = {
    db.run(sqlu"DELETE FROM outbox_events").futureValue
    db.run(sqlu"DELETE FROM dead_letter_events").futureValue
  }

  "OutboxProcessor" should {

    "process events successfully" in {
      cleanDatabase()

      val successPublisher = new EventPublisher {
        override def publish(event: OutboxEvent): Future[PublishResult] =
          Future.successful(PublishResult.Success)
      }

      val processor = testKit.spawn(
        OutboxProcessor(
          db                  = db,
          publisher           = successPublisher,
          outboxRepo          = outboxRepo,
          pollInterval        = 1.second,
          batchSize           = 10,
          maxRetries          = 2,
          useListenNotify     = false,
          staleCleanupEnabled = false
        )
      )

      val outboxEvent = OutboxEvent(
        aggregateId = "1",
        eventType   = "OrderCreated",
        payload     = """{"orderId":1,"customerId":"test@example.com","totalAmount":100}"""
      ).withIdempotencyKey
      val eventId = db.run(outboxRepo.insert(outboxEvent)).futureValue

      processor ! ProcessUnhandledEvent

      Thread.sleep(1000)

      val processed = db.run(outboxRepo.find(eventId)).futureValue
      processed.status shouldBe EventStatus.Processed

      val probe = testKit.createTestProbe[OutboxProcessor.Stopped.type]()
      processor ! Stop(probe.ref)
      probe.expectMessage(OutboxProcessor.Stopped)
    }

    "retry on retryable failures" in {
      cleanDatabase()

      @volatile var attemptCount = 0

      val retryablePublisher = new EventPublisher {
        override def publish(event: OutboxEvent): Future[PublishResult] = {
          attemptCount += 1
          Future.successful(PublishResult.Retryable("Connection timeout"))
        }
      }

      val processor = testKit.spawn(
        OutboxProcessor(
          db                  = db,
          publisher           = retryablePublisher,
          outboxRepo          = outboxRepo,
          pollInterval        = 500.millis,
          batchSize           = 10,
          maxRetries          = 2,
          useListenNotify     = false,
          staleCleanupEnabled = false
        )
      )

      val outboxEvent = OutboxEvent(
        aggregateId = "1",
        eventType   = "OrderCreated",
        payload     = """{"orderId":1,"customerId":"test@example.com","totalAmount":100}"""
      ).withIdempotencyKey
      val eventId = db.run(outboxRepo.insert(outboxEvent)).futureValue

      processor ! ProcessUnhandledEvent
      Thread.sleep(500)
      processor ! ProcessUnhandledEvent
      Thread.sleep(500)
      processor ! ProcessUnhandledEvent
      Thread.sleep(500)

      attemptCount should be >= 2

      val dlqCount = db
        .run(
          sql"SELECT COUNT(*) FROM dead_letter_events WHERE original_event_id = $eventId"
            .as[Int]
            .head
        )
        .futureValue
      dlqCount shouldBe 1

      val probe = testKit.createTestProbe[OutboxProcessor.Stopped.type]()
      processor ! Stop(probe.ref)
      probe.expectMessage(OutboxProcessor.Stopped)
    }

    "move to DLQ immediately on non-retryable failures" in {
      cleanDatabase()

      val nonRetryablePublisher = new EventPublisher {
        override def publish(event: OutboxEvent): Future[PublishResult] =
          Future.successful(PublishResult.NonRetryable("Invalid payload"))
      }

      val processor = testKit.spawn(
        OutboxProcessor(
          db                  = db,
          publisher           = nonRetryablePublisher,
          outboxRepo          = outboxRepo,
          pollInterval        = 1.second,
          batchSize           = 10,
          maxRetries          = 2,
          useListenNotify     = false,
          staleCleanupEnabled = false
        )
      )

      val outboxEvent = OutboxEvent(
        aggregateId = "1",
        eventType   = "OrderCreated",
        payload     = """{"orderId":1,"customerId":"test@example.com","totalAmount":100}"""
      ).withIdempotencyKey
      val eventId = db.run(outboxRepo.insert(outboxEvent)).futureValue

      processor ! ProcessUnhandledEvent

      Thread.sleep(1000)

      val processed = db.run(outboxRepo.find(eventId)).futureValue
      processed.status shouldBe EventStatus.Processed
      processed.movedToDlq shouldBe true
      processed.retryCount shouldBe 0 // No retries attempted

      val probe = testKit.createTestProbe[OutboxProcessor.Stopped.type]()
      processor ! Stop(probe.ref)
      probe.expectMessage(OutboxProcessor.Stopped)
    }

    "process batch of events" in {
      cleanDatabase()

      val successPublisher = new EventPublisher {
        override def publish(event: OutboxEvent): Future[PublishResult] =
          Future.successful(PublishResult.Success)
      }

      val processor = testKit.spawn(
        OutboxProcessor(
          db                  = db,
          publisher           = successPublisher,
          outboxRepo          = outboxRepo,
          pollInterval        = 1.second,
          batchSize           = 5,
          maxRetries          = 2,
          useListenNotify     = false,
          staleCleanupEnabled = false
        )
      )

      (1 to 5).foreach { i =>
        val outboxEvent = OutboxEvent(
          aggregateId = i.toString,
          eventType   = "OrderCreated",
          payload     = s"""{"orderId":$i,"customerId":"test$i@example.com","totalAmount":100}"""
        ).withIdempotencyKey
        db.run(outboxRepo.insert(outboxEvent)).futureValue
      }

      processor ! ProcessUnhandledEvent

      Thread.sleep(2000)

      val processedCount = db
        .run(
          sql"SELECT COUNT(*) FROM outbox_events WHERE status = 'PROCESSED'"
            .as[Int]
            .head
        )
        .futureValue
      processedCount shouldBe 5

      val probe = testKit.createTestProbe[OutboxProcessor.Stopped.type]()
      processor ! Stop(probe.ref)
      probe.expectMessage(OutboxProcessor.Stopped)
    }
  }
}
