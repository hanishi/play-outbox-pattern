package repositories

import models.*
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerSuite
import play.api.Configuration
import play.api.db.slick.DatabaseConfigProvider
import repositories.OutboxEventsPostgresProfile.api.*
import slick.jdbc.JdbcBackend.Database

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.*

class OutboxRepositorySpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with GuiceOneAppPerSuite
    with BeforeAndAfterEach {

  private lazy val dbConfigProvider = app.injector.instanceOf[DatabaseConfigProvider]
  private lazy val db = dbConfigProvider.get[OutboxEventsPostgresProfile].db.asInstanceOf[Database]
  private lazy val config  = app.injector.instanceOf[Configuration]
  private lazy val dlqRepo = new DeadLetterRepository(config)(using global)
  private lazy val repo    = new OutboxRepository(dlqRepo, config)(using global)

  // Disable auto-start of event processing for repository tests
  override def fakeApplication(): play.api.Application =
    new play.api.inject.guice.GuiceApplicationBuilder()
      .configure("outbox.autoStart" -> false)
      .build()

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 5.seconds, interval = 50.millis)

  override def beforeEach(): Unit = {
    // Clean up outbox_events table before each test
    db.run(sqlu"DELETE FROM outbox_events").futureValue
    db.run(sqlu"DELETE FROM dead_letter_events").futureValue
  }

  "OutboxRepository" should {

    "save an event" in {
      val event = OrderCreatedEvent(
        orderId      = 1L,
        customerId   = "test@example.com",
        totalAmount  = BigDecimal(100.00),
        shippingType = "domestic",
        timestamp    = Instant.now()
      )

      val result = db.run(repo.insert(toOutboxEvent(event))).futureValue

      result should be > 0L

      // Verify event was saved
      val saved = db.run(repo.find(result)).futureValue
      saved.eventType shouldBe "OrderCreated"
      saved.status shouldBe EventStatus.Pending
      saved.retryCount shouldBe 0
    }

    "find and claim unprocessed events atomically" in {
      // Insert 3 pending events
      val event1 = createTestEvent(1L, "customer-1")
      val event2 = createTestEvent(2L, "customer-1")
      val event3 = createTestEvent(3L, "customer-2")

      db.run(repo.insert(toOutboxEvent(event1))).futureValue
      db.run(repo.insert(toOutboxEvent(event2))).futureValue
      db.run(repo.insert(toOutboxEvent(event3))).futureValue

      // Claim batch of 2
      val claimed = db.run(repo.findAndClaimUnprocessed(limit = 2)).futureValue

      claimed should have size 2
      claimed.foreach { event =>
        event.status shouldBe EventStatus.Processing
      }

      // Verify remaining event is still PENDING
      val remaining = db
        .run(
          sql"SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'"
            .as[Int]
            .head
        )
        .futureValue
      remaining shouldBe 1
    }

    "not claim same event twice (FOR UPDATE SKIP LOCKED)" in {
      val event = createTestEvent(1L, "customer-1")
      db.run(repo.insert(toOutboxEvent(event))).futureValue

      // Simulate two concurrent workers claiming
      val claim1 = db.run(repo.findAndClaimUnprocessed(1))
      val claim2 = db.run(repo.findAndClaimUnprocessed(1))

      val results = for {
        c1 <- claim1
        c2 <- claim2
      } yield (c1, c2)

      val (claimed1, claimed2) = results.futureValue

      // Only one worker should get the event
      (claimed1.size + claimed2.size) shouldBe 1
    }

    "increment retry count and reset to PENDING" in {
      val event   = createTestEvent(1L, "customer-1")
      val eventId = db.run(repo.insert(toOutboxEvent(event))).futureValue

      // Claim the event (status -> PROCESSING)
      db.run(repo.findAndClaimUnprocessed(1)).futureValue

      // Increment retry count
      val error       = "Connection timeout"
      val nextRetryAt = Instant.now().plusSeconds(2)
      db.run(repo.incrementRetryCount(eventId, currentRetryCount = 0, error, nextRetryAt))
        .futureValue

      // Verify status is back to PENDING with retry_count = 1
      val updated = db.run(repo.find(eventId)).futureValue
      updated.status shouldBe EventStatus.Pending
      updated.retryCount shouldBe 1
      updated.lastError shouldBe Some(error)
    }

    "not overwrite DLQ events when incrementing retry count (regression test)" in {
      val event   = createTestEvent(1L, "customer-1")
      val eventId = db.run(repo.insert(toOutboxEvent(event))).futureValue

      // Move event to DLQ
      val outboxEvent = db.run(repo.find(eventId)).futureValue
      db.run(repo.moveToDLQ(outboxEvent, "MAX_RETRIES_EXCEEDED", "Test DLQ")).futureValue

      // Verify it's in DLQ
      val afterDLQ = db.run(repo.find(eventId)).futureValue
      afterDLQ.status shouldBe EventStatus.Processed
      afterDLQ.movedToDlq shouldBe true

      // Try to increment retry count (should not affect DLQ event)
      val nextRetryAt  = Instant.now().plusSeconds(2)
      val affectedRows = db
        .run(
          repo.incrementRetryCount(eventId, currentRetryCount = 0, "Should not apply", nextRetryAt)
        )
        .futureValue

      affectedRows shouldBe 0 // Should not update

      // Verify event is still in DLQ state
      val stillDLQ = db.run(repo.find(eventId)).futureValue
      stillDLQ.status shouldBe EventStatus.Processed
      stillDLQ.movedToDlq shouldBe true
    }

    "move event to DLQ" in {
      val event   = createTestEvent(1L, "customer-1")
      val eventId = db.run(repo.insert(toOutboxEvent(event))).futureValue

      val outboxEvent = db.run(repo.find(eventId)).futureValue

      // Move to DLQ
      db.run(repo.moveToDLQ(outboxEvent, "MAX_RETRIES_EXCEEDED", "Failed 3 times")).futureValue

      // Verify original event is marked
      val original = db.run(repo.find(eventId)).futureValue
      original.status shouldBe EventStatus.Processed
      original.movedToDlq shouldBe true

      // Verify DLQ entry exists
      val dlqCount = db
        .run(
          sql"SELECT COUNT(*) FROM dead_letter_events WHERE original_event_id = $eventId"
            .as[Int]
            .head
        )
        .futureValue
      dlqCount shouldBe 1
    }

    "reset stale PROCESSING events to PENDING" in {
      val event   = createTestEvent(1L, "customer-1")
      val eventId = db.run(repo.insert(toOutboxEvent(event))).futureValue

      db.run(repo.findAndClaimUnprocessed(1)).futureValue

      db.run(
        sqlu"UPDATE outbox_events SET status_changed_at = NOW() - INTERVAL '10 minutes' WHERE id = $eventId"
      ).futureValue

      val resetCount = db.run(repo.resetStaleProcessingEvents(timeoutMinutes = 5)).futureValue

      resetCount shouldBe 1

      val reset = db.run(repo.find(eventId)).futureValue
      reset.status shouldBe EventStatus.Pending
    }

    "not reset recent PROCESSING events" in {
      val initialCount = db.run(sql"SELECT COUNT(*) FROM outbox_events".as[Int].head).futureValue
      initialCount shouldBe 0

      val event   = createTestEvent(1L, "customer-1")
      val eventId = db.run(repo.insert(toOutboxEvent(event))).futureValue

      db.run(sqlu"UPDATE outbox_events SET created_at = NOW() WHERE id = $eventId").futureValue

      db.run(repo.findAndClaimUnprocessed(1)).futureValue

      val beforeReset = db.run(repo.find(eventId)).futureValue
      beforeReset.status shouldBe EventStatus.Processing

      val resetCount = db.run(repo.resetStaleProcessingEvents(timeoutMinutes = 5)).futureValue

      resetCount shouldBe 0

      val afterReset = db.run(repo.find(eventId)).futureValue
      afterReset.status shouldBe EventStatus.Processing
    }

    "count pending events" in {
      val event1 = createTestEvent(1L, "customer-1")
      val event2 = createTestEvent(2L, "customer-2")
      val event3 = createTestEvent(3L, "customer-3")

      val id1 = db.run(repo.insert(toOutboxEvent(event1))).futureValue
      val id2 = db.run(repo.insert(toOutboxEvent(event2))).futureValue
      val id3 = db.run(repo.insert(toOutboxEvent(event3))).futureValue

      db.run(
        sqlu"UPDATE outbox_events SET status = 'PROCESSED' WHERE aggregate_id=${event1.aggregateId}"
      ).futureValue

      val pendingCount = db.run(repo.countPending).futureValue

      pendingCount shouldBe 2
    }
  }

  private def createTestEvent(orderId: Long, customerId: String): DomainEvent =
    OrderCreatedEvent(
      orderId      = orderId,
      customerId   = customerId,
      totalAmount  = BigDecimal(100.00),
      shippingType = "domestic",
      timestamp    = Instant.now()
    )

  private def toOutboxEvent(event: DomainEvent): OutboxEvent =
    OutboxEvent(
      aggregateId = event.aggregateId,
      eventType   = event.eventType,
      payloads    = Map(event.eventType -> DestinationConfig(payload = Some(event.toPayload)))
    ).withIdempotencyKey
}
