package actors

import models.*
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import play.api.Logger
import play.api.libs.json.*
import publishers.*
import repositories.*
import slick.jdbc.JdbcBackend.Database

import java.time.*
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.util.*

/** Dead Letter Queue (DLQ) processor that performs automatic compensation for failed forward events.
  *
  * == Purpose ==
  * When a forward event (e.g., OrderCreated) fails after max retries in OutboxProcessor,
  * it's moved to the DLQ. This processor automatically reverts all successful API calls
  * for that aggregate, implementing the Saga compensation pattern with LIFO ordering.
  *
  * == Complete Compensation Flow ==
  *
  * '''1. Forward Event Fails (OutboxProcessor):'''
  * {{{
  * # OrderCreated event calls (fanout order):
  * POST /api/inventory/reserve       → 200 OK (reservationId: RES-456)
  * POST /api/fraud/check             → 200 OK (riskScore: 25)
  * POST /api/domestic-shipping       → 200 OK (shipmentId: SHIP-789)
  * POST /api/billing                 → 503 Service Unavailable (after 3 retries)
  *
  * # OutboxProcessor moves to DLQ:
  * INSERT INTO dead_letter_events (
  *   original_event_id = 123,
  *   aggregate_id = '456',
  *   event_type = 'OrderCreated',
  *   status = 'PENDING',
  *   reason = 'MAX_RETRIES_EXCEEDED',
  *   error = 'HTTP 503 Service Unavailable',
  *   revert_retry_count = 0
  * ) RETURNING id;  -- Returns DLQ id: 789
  *
  * # Trigger DLQProcessor:
  * dlqProcessor ! ProcessPendingDLQ
  * }}}
  *
  * '''2. DLQ Claims Event:'''
  * {{{
  * SELECT * FROM dead_letter_events
  * WHERE status = 'PENDING'
  *   AND event_type NOT LIKE '!%'  -- Exclude manual revert events
  *   AND (next_retry_at IS NULL OR next_retry_at <= NOW())
  * ORDER BY created_at
  * LIMIT 100
  * FOR UPDATE SKIP LOCKED;  -- Concurrency-safe
  *
  * UPDATE dead_letter_events SET status = 'PROCESSING' WHERE id = 789;
  * }}}
  *
  * '''3. Query Successful Forward Results (LIFO order):'''
  * {{{
  * # Get all successful API calls for this aggregate
  * SELECT *
  * FROM aggregate_results
  * WHERE aggregate_id = '456'
  *   AND event_type = 'OrderCreated'
  *   AND success = true
  *   AND destination NOT LIKE '%.revert'  -- Exclude previous compensation attempts
  * ORDER BY fanout_order DESC;  -- LIFO: reverse order of forward fanout
  *
  * # Returns (LIFO order):
  * # [
  * #   {destination: "shipping",   fanout_order: 2, endpointUrl: ".../domestic-shipping", response: {"shipmentId": "SHIP-789"}},
  * #   {destination: "fraudCheck", fanout_order: 1, endpointUrl: ".../fraud/check",       response: {"riskScore": 25}},
  * #   {destination: "inventory",  fanout_order: 0, endpointUrl: ".../inventory/reserve",  response: {"reservationId": "RES-456"}}
  * # ]
  * }}}
  *
  * '''4. Check Which Are Already Compensated (Idempotency):'''
  * {{{
  * # For each successful forward result, check if already reverted:
  * SELECT * FROM aggregate_results
  * WHERE aggregate_id = '456'
  *   AND event_type = 'OrderCreated:REVERT'
  *   AND destination = 'shipping.revert'
  *   AND success = true;
  * # Returns: None (not yet compensated)
  *
  * SELECT * FROM aggregate_results
  * WHERE aggregate_id = '456'
  *   AND event_type = 'OrderCreated:REVERT'
  *   AND destination = 'inventory.revert'
  *   AND success = true;
  * # Returns: None (not yet compensated)
  * }}}
  *
  * '''5. Build Revert Endpoints from Configuration:'''
  * {{{
  * # For shipping (LIFO: first to revert):
  * # Forward result:
  * #   Request:  {"customerId": "C-123", "shippingType": "domestic", "orderId": "456"}
  * #   Response: {"shipmentId": "SHIP-789", "orderId": "456", "estimatedDelivery": "2024-01-15"}
  * #   endpointUrl: "http://localhost:9000/api/domestic-shipping"
  *
  * # Revert config (from application.conf):
  * #   shipping.routes[0].revert {
  * #     url = "http://localhost:9000/api/domestic-shipping/{shipmentId}/cancel"
  * #     method = "POST"
  * #     extract {
  * #       shipmentId = "response:$.shipmentId"       # Extract from forward response
  * #       orderId = "response:$.orderId"             # Extract from forward response
  * #       customerId = "request:$.customerId"        # Extract from forward request
  * #     }
  * #     payload = """{"reason": "payment_failed", "shipmentId": "{shipmentId}", "orderId": "{orderId}", "customerId": "{customerId}"}"""
  * #   }
  *
  * # JSONPath extraction:
  * #   shipmentId = response:$.shipmentId   → "SHIP-789"
  * #   orderId = response:$.orderId         → "456"
  * #   customerId = request:$.customerId    → "C-123"
  *
  * # Built revert endpoint:
  * #   URL: POST http://localhost:9000/api/domestic-shipping/SHIP-789/cancel
  * #   Payload: {"reason": "payment_failed", "shipmentId": "SHIP-789", "orderId": "456", "customerId": "C-123"}
  *
  * # For fraudCheck:
  * #   No revert config (read-only service, no compensation needed)
  * #   Gracefully skipped
  *
  * # For inventory (LIFO: second to revert):
  * # Forward result:
  * #   Response: {"reservationId": "RES-456", "quantity": 2}
  *
  * # Revert config:
  * #   inventory.revert {
  * #     url = "http://localhost:9000/api/inventory/{reservationId}/release"
  * #     method = "DELETE"
  * #     extract {
  * #       reservationId = "response:$.reservationId"
  * #     }
  * #   }
  *
  * # Built revert endpoint:
  * #   URL: DELETE http://localhost:9000/api/inventory/RES-456/release
  * }}}
  *
  * '''6. Publish Revert Endpoints (LIFO order):'''
  * {{{
  * # Call revert APIs in LIFO order:
  * POST http://localhost:9000/api/domestic-shipping/SHIP-789/cancel  → 200 OK
  * # fraudCheck skipped (no revert config)
  * DELETE http://localhost:9000/api/inventory/RES-456/release        → 200 OK
  *
  * # Save compensation results:
  * INSERT INTO aggregate_results (
  *   aggregate_id = '456',
  *   event_type = 'OrderCreated:REVERT',
  *   destination = 'shipping.revert',
  *   endpoint_url = 'http://localhost:9000/api/domestic-shipping/SHIP-789/cancel',
  *   request_payload = '{"reason": "payment_failed", "shipmentId": "SHIP-789", ...}',
  *   response_payload = '{"cancelled": true, "refundInitiated": true}',
  *   fanout_order = 0,  -- LIFO: shipping is first to revert
  *   success = true
  * );
  * INSERT INTO aggregate_results (
  *   aggregate_id = '456',
  *   event_type = 'OrderCreated:REVERT',
  *   destination = 'inventory.revert',
  *   endpoint_url = 'http://localhost:9000/api/inventory/RES-456/release',
  *   request_payload = NULL,  -- DELETE method, no body
  *   response_payload = '{"released": true, "quantityReturned": 2}',
  *   fanout_order = 1,  -- LIFO: inventory is second to revert
  *   success = true
  * );
  * }}}
  *
  * '''7. Mark DLQ Event as PROCESSED:'''
  * {{{
  * UPDATE dead_letter_events
  * SET status = 'PROCESSED', status_changed_at = NOW()
  * WHERE id = 789;
  * }}}
  *
  * == Partial Failure Handling ==
  *
  * '''Scenario: Shipping Revert Succeeds, Inventory Revert Fails:'''
  * {{{
  * # Step 1: Shipping compensation succeeds
  * POST /api/domestic-shipping/SHIP-789/cancel  → 200 OK
  * INSERT INTO aggregate_results (destination='shipping.revert', success=true);
  *
  * # Step 2: Inventory compensation fails
  * DELETE /api/inventory/RES-456/release        → 500 Internal Server Error
  *
  * # Step 3: DLQProcessor increments revert_retry_count
  * UPDATE dead_letter_events
  * SET revert_retry_count = 1,
  *     next_retry_at = NOW() + INTERVAL '2 seconds',
  *     last_error = 'HTTP 500 Internal Server Error',
  *     status = 'PENDING'  -- Reset for retry
  * WHERE id = 789;
  *
  * # Step 4: Retry after 2 seconds (idempotency check)
  * # Query already compensated:
  * SELECT * FROM aggregate_results
  * WHERE aggregate_id='456' AND destination='shipping.revert' AND success=true;
  * # Returns: shipping.revert (already compensated)
  *
  * SELECT * FROM aggregate_results
  * WHERE aggregate_id='456' AND destination='inventory.revert' AND success=true;
  * # Returns: None (needs retry)
  *
  * # Only retry inventory (shipping skipped due to idempotency):
  * DELETE /api/inventory/RES-456/release        → 200 OK (success on retry)
  * INSERT INTO aggregate_results (destination='inventory.revert', success=true);
  *
  * # Step 5: Mark DLQ event as PROCESSED
  * UPDATE dead_letter_events SET status = 'PROCESSED' WHERE id = 789;
  * }}}
  *
  * '''Scenario: All Retries Exhausted:'''
  * {{{
  * # After maxRetries (default: 3) attempts, inventory still failing:
  * UPDATE dead_letter_events
  * SET status = 'FAILED',
  *     revert_retry_count = 3,
  *     last_error = 'HTTP 500 Internal Server Error'
  * WHERE id = 789;
  *
  * # Manual intervention required:
  * # 1. Check logs for failure reason
  * # 2. Fix underlying issue (e.g., inventory service down)
  * # 3. Manually retry compensation:
  * UPDATE dead_letter_events
  * SET status = 'PENDING', revert_retry_count = 0, next_retry_at = NULL
  * WHERE id = 789;
  * }}}
  *
  * == Key Features ==
  *
  * '''LIFO Compensation Order (Saga Pattern):'''
  * {{{
  * # Forward fanout order (from configuration):
  * OrderCreated = ["inventory", "fraudCheck", "shipping", "billing"]
  * # fanout_order values: 0, 1, 2, 3
  *
  * # Revert order (automatic reverse):
  * # Query: ORDER BY fanout_order DESC
  * # Result: ["shipping"(2), "fraudCheck"(1), "inventory"(0)]
  * # billing(3) not included (it failed, so no compensation needed)
  *
  * # Why LIFO?
  * # - Ensures proper unwinding of dependencies
  * # - Example: Must cancel shipment BEFORE releasing inventory
  * # - Prevents inconsistent states (e.g., inventory available but shipment still active)
  * }}}
  *
  * '''Idempotency (Prevents Double-Compensation):'''
  * {{{
  * # Scenario: DLQ processing fails partway
  * # Attempt 1: shipping.revert succeeds, inventory.revert fails
  * # Attempt 2: Check existing compensations before retrying
  *
  * # Query:
  * SELECT * FROM aggregate_results
  * WHERE aggregate_id = '456'
  *   AND event_type = 'OrderCreated:REVERT'
  *   AND destination = 'shipping.revert'
  *   AND success = true;
  *
  * # Result: shipping.revert found → skip
  * # Only retry: inventory.revert
  *
  * # Prevents:
  * # - Double-cancellation of shipments
  * # - Multiple refund attempts
  * # - Duplicate inventory releases
  * }}}
  *
  * '''Graceful Handling of Read-Only Services:'''
  * {{{
  * # fraudCheck destination:
  * # - No revert config in application.conf
  * # - Read-only operation (no side effects to compensate)
  * # - DLQProcessor gracefully skips
  *
  * # Log output:
  * # [info] Found revert config for shipping
  * # [info] No revert config for fraudCheck, skipping (read-only service)
  * # [info] Found revert config for inventory
  * }}}
  */
object DLQProcessor {

  def apply(
      publisher: HttpEventPublisher,
      dlqRepo: DeadLetterRepository,
      resultRepo: DestinationResultRepository,
      eventRouter: EventRouter,
      pollInterval: FiniteDuration = 5.seconds,
      batchSize: Int               = 100,
      maxRetries: Int              = 5,
      useListenNotify: Boolean     = false
  )(using db: Database): Behavior[Command] = Behaviors.setup { context =>
    context.log.info(
      s"DLQProcessor starting (maxRetries: $maxRetries, useListenNotify: $useListenNotify)"
    )

    Behaviors.withTimers { timers =>
      Behaviors.withStash(Int.MaxValue) { stash =>
        timers.startSingleTimer(ProcessPendingDLQ, 1.second)

        new DLQProcessor(
          db,
          publisher,
          dlqRepo,
          resultRepo,
          eventRouter,
          pollInterval,
          batchSize,
          maxRetries,
          useListenNotify,
          context,
          stash,
          timers
        ).idle
      }
    }
  }

  sealed trait Command

  final case class Stop(replyTo: ActorRef[Stopped.type]) extends Command

  private final case class ProcessingComplete(stats: EventProcessor.Stats) extends Command

  private final case class ProcessingFailed(ex: Throwable) extends Command

  private final case class CheckNextDue(nextDue: Option[Instant]) extends Command

  private final case class CheckNextDueFailed(ex: Throwable) extends Command

  case object Stopped

  case object ProcessPendingDLQ extends Command
}

class DLQProcessor(
    db: Database,
    publisher: HttpEventPublisher,
    dlqRepo: DeadLetterRepository,
    resultRepo: DestinationResultRepository,
    eventRouter: EventRouter,
    protected val pollInterval: FiniteDuration,
    batchSize: Int,
    maxRetries: Int,
    useListenNotify: Boolean,
    protected val context: ActorContext[DLQProcessor.Command],
    protected val stash: StashBuffer[DLQProcessor.Command],
    protected val timers: TimerScheduler[DLQProcessor.Command]
) extends EventProcessor.Base[DLQProcessor.Command] {

  import DLQProcessor.*

  private val log = context.log

  private val stopHandler: PartialFunction[Command, Behavior[Command]] = { case Stop(replyTo) =>
    context.log.info("Stopping DLQProcessor")
    timers.cancelAll()
    replyTo ! Stopped
    Behaviors.stopped
  }

  override protected def logger: Logger = Logger(this.getClass)

  private def scheduleNext(): Unit = scheduleNext(ProcessPendingDLQ)

  private def idle: Behavior[Command] = Behaviors.receiveMessage {
    case ProcessPendingDLQ =>
      context.log.debug("Processing pending DLQ events for revert")
      pipeToSelf(processDLQBatch)(stats => ProcessingComplete(stats), e => ProcessingFailed(e))
      stash.unstashAll(processing)
    case other =>
      stopHandler.applyOrElse(
        other,
        command => {
          stash.stash(command)
          Behaviors.same
        }
      )
  }

  private def processing: Behavior[Command] = Behaviors.receiveMessage {
    case ProcessingComplete(stats) =>
      if (stats.processed > 0 || stats.failed > 0) {
        context.log.info(
          s"DLQ Batch complete - Fetched: ${stats.fetched}, ✓ Reverted: ${stats.processed}, ❌ Failed: ${stats.failed}"
        )
      }

      if (stats.fetched >= batchSize) {
        context.log.info("Full batch processed, scheduling immediate reprocessing")
        timers.startSingleTimer(ProcessPendingDLQ, 100.millis)
        stash.unstashAll(idle)
      } else {
        // Check when the next event is due for scheduling
        pipeToSelf(db.run(dlqRepo.getNextDueTime))(
          nextDue => CheckNextDue(nextDue),
          ex => CheckNextDueFailed(ex)
        )
        stash.unstashAll(checkingNextDue)
      }

    case ProcessingFailed(ex) =>
      context.log.error("DLQ processing failed", ex)
      scheduleNext()
      stash.unstashAll(idle)
    case other =>
      stopHandler.applyOrElse(
        other,
        command => {
          stash.stash(command)
          Behaviors.same
        }
      )
  }

  private def checkingNextDue: Behavior[Command] = Behaviors.receiveMessage {
    case CheckNextDue(nextDue) =>
      nextDue match {
        case None =>
          context.log.debug("No more pending DLQ events")
          if (!useListenNotify) {
            scheduleNext()
          }

        case Some(dueTime) if dueTime.isBefore(Instant.now()) =>
          context.log.info("More DLQ events ready now, scheduling immediate reprocessing")
          timers.startSingleTimer(ProcessPendingDLQ, 100.millis)

        case Some(dueTime) =>
          val delayMillis = dueTime.toEpochMilli - Instant.now().toEpochMilli
          context.log.info(
            s"Next DLQ event ready in ${delayMillis}ms, scheduling check at exact time"
          )
          timers.startSingleTimer(ProcessPendingDLQ, delayMillis.millis)
      }
      stash.unstashAll(idle)

    case CheckNextDueFailed(ex) =>
      context.log.error("Failed to check next due time for DLQ", ex)
      scheduleNext()
      stash.unstashAll(idle)

    case other =>
      stopHandler.applyOrElse(
        other,
        command => {
          stash.stash(command)
          Behaviors.same
        }
      )
  }

  private def processDLQBatch: Future[EventProcessor.Stats] =
    processBatch(
      fetchBatch    = db.run(dlqRepo.findAndClaimPendingForRevert(batchSize)),
      processEvent  = revertDLQEvent,
      eventTypeName = "DLQ",
      getEventId    = _.id
    )

  private def revertDLQEvent(dlqEvent: DeadLetterEvent): Future[Boolean] = {
    log.info(s"Processing DLQ event ${dlqEvent.id} (aggregate: ${dlqEvent.aggregateId})")

    // Query only forward flow results (exclude :REVERT events) to determine what needs reverting
    db.run(
      resultRepo.findByAggregateId(dlqEvent.aggregateId, Result.Success, includeReverts = false)
    ).flatMap { successful =>
      if (successful.isEmpty) {
        log.info(s"No successful results to revert for aggregate ${dlqEvent.aggregateId}")
        db.run(dlqRepo.markProcessed(dlqEvent.id)).map(_ => true)
      } else {
        log.info(
          s"Found ${successful.size} successful results to revert for aggregate ${dlqEvent.aggregateId}"
        )
        successful.foreach { result =>
          val icon = if (result.success) "✓" else "❌"
          log.info(s"  $icon ${result.destination}: ${result.endpointUrl}")
        }
        publishRevertEvent(dlqEvent, successful)
      }
    }.recoverWith { case ex =>
      log.error(s"Unexpected error processing DLQ event ${dlqEvent.id}", ex)
      handleRevertFailure(dlqEvent, ex.getMessage)
    }
  }

  private def publishRevertEvent(
      dlqEvent: DeadLetterEvent,
      successfulResults: Seq[AggregateResult]
  ): Future[Boolean] = revertSuccessfulDestinations(dlqEvent, successfulResults, List.empty)
    .flatMap {
      case PublishResult.Success =>
        log.info(
          s"Successfully reverted all destinations for DLQ event ${dlqEvent.id} (aggregate: ${dlqEvent.aggregateId})"
        )
        db.run(dlqRepo.markProcessed(dlqEvent.id)).map(_ => true)

      case PublishResult.Retryable(error, retryAfter) =>
        log.warn(
          s"❌ DLQ event ${dlqEvent.id} revert failed (retryable): $error, retry after: $retryAfter"
        )
        handleRevertFailure(dlqEvent, error, retryAfter)

      case PublishResult.NonRetryable(error) =>
        log.error(s"❌ DLQ event ${dlqEvent.id} revert failed (non-retryable): $error")
        handleRevertFailure(dlqEvent, error, isRetryable = false)
    }

  /** Reverts successful destinations by building revert endpoints from route configs.
    */
  private def revertSuccessfulDestinations(
      dlqEvent: DeadLetterEvent,
      successfulResults: Seq[AggregateResult],
      endpoints: List[HttpEndpointConfig]
  ): Future[PublishResult] = for {
    alreadyCompensated <- checkAlreadyCompensated(dlqEvent.aggregateId, successfulResults)
    resultsToRevert = successfulResults.filterNot(r => alreadyCompensated.contains(r.destination))
    result <-
      if (resultsToRevert.isEmpty && alreadyCompensated.nonEmpty) {
        log.info(s"All destinations already compensated for aggregate ${dlqEvent.aggregateId}")
        Future.successful(PublishResult.Success)
      } else publishRevertEndpoints(dlqEvent, resultsToRevert)
  } yield result

  /** Check which destinations have already been compensated. */
  private def checkAlreadyCompensated(
      aggregateId: String,
      successfulResults: Seq[AggregateResult]
  ): Future[Set[String]] =
    Future
      .sequence(
        successfulResults.map { result =>
          db.run(resultRepo.findSuccessfulRevert(aggregateId, result.destination))
            .map(alreadyCompensated => (result.destination, alreadyCompensated.isDefined))
        }
      )
      .map { checksResults =>
        val compensated = checksResults.filter(_._2).map(_._1).toSet
        compensated.foreach { dest =>
          log.info(s"Destination $dest already compensated, skipping revert")
        }
        compensated
      }

  /** Publish revert endpoints for destinations that need compensation. */
  private def publishRevertEndpoints(
      dlqEvent: DeadLetterEvent,
      resultsToRevert: Seq[AggregateResult]
  ): Future[PublishResult] =
    (resultsToRevert, eventRouter) match {
      case (results, router: ConfigurationEventRouter) =>
        val baseEventType = dlqEvent.eventType.replace(":REVERT", "").stripPrefix("!")
        val revertOrder   = getRevertOrderForEvent(router, baseEventType, results)

        buildRevertEndpointsInOrder(dlqEvent, results, router, revertOrder) match {
          case Nil =>
            log.info(s"No revert endpoints to execute for aggregate ${dlqEvent.aggregateId}")
            Future.successful(PublishResult.Success)
          case revert =>
            publishRevertEvent(dlqEvent, revert)
        }

      case _ =>
        log.warn("EventRouter is not ConfigurationEventRouter, cannot perform automatic revert")
        Future.successful(PublishResult.Success)
    }

  /** Get LIFO revert order from the router configuration. */
  private def getRevertOrderForEvent(
      router: ConfigurationEventRouter,
      baseEventType: String,
      resultsToRevert: Seq[AggregateResult]
  ): List[String] = {
    val revertOrder = router.getFanoutOrder(s"!$baseEventType").getOrElse {
      log.warn(
        s"No fanout configuration found for !$baseEventType, using forward results in reverse order"
      )
      resultsToRevert.reverse.map(r => s"${r.destination}.revert").toList
    }
    log.info(s"DLQ revert order for $baseEventType: ${revertOrder.mkString(", ")}")
    revertOrder
  }

  /** Build revert endpoints in LIFO order. */
  private def buildRevertEndpointsInOrder(
      dlqEvent: DeadLetterEvent,
      resultsToRevert: Seq[AggregateResult],
      router: ConfigurationEventRouter,
      revertOrder: List[String]
  ): List[(HttpEndpointConfig, Option[JsValue])] = {
    val resultsByDest = resultsToRevert.map(r => r.destination -> r).toMap

    for {
      revertDestName <- revertOrder
      destName = revertDestName.replace(".revert", "")
      result <- resultsByDest.get(destName)
      endpoint <- buildRevertEndpointForDestination(dlqEvent, result, router, destName)
    } yield endpoint
  }

  /** Build revert endpoint for a single destination. */
  private def buildRevertEndpointForDestination(
      dlqEvent: DeadLetterEvent,
      result: AggregateResult,
      router: ConfigurationEventRouter,
      destName: String
  ): Option[(HttpEndpointConfig, Option[JsValue])] = {
    val endpoint = router.routeEvent(
      OutboxEvent(aggregateId = "", eventType = destName, payloads = Map.empty)
    )

    log.info(
      s"Building revert for ${result.destination}, routes.size=${endpoint.routes.size}, endpointUrl='${result.endpointUrl}'"
    )

    findRevertConfigForDestination(result, endpoint).flatMap { revertConfig =>
      log.info(s"  Found revert config for ${result.destination}")
      buildRevertEndpointAndPayload(dlqEvent, result, revertConfig, endpoint) match {
        case Success((revertEndpoint, payload)) =>
          revertEndpoint.url.foreach(url =>
            log.info(s"  Successfully built revert endpoint for ${result.destination}: $url")
          )
          Some((revertEndpoint, payload))
        case Failure(ex) =>
          log.error(
            s"Failed to build revert endpoint for ${result.destination}: ${ex.getMessage}"
          )
          None
      }
    }
  }

  /** Find revert config for a destination (handles multi-route vs single-route). */
  private def findRevertConfigForDestination(
      result: AggregateResult,
      endpoint: HttpEndpointConfig
  ): Option[RevertConfig] =
    if (endpoint.routes.nonEmpty) {
      // Multi-route destination: match by URL from forward result
      if (result.endpointUrl.nonEmpty) {
        val matchingRoute = endpoint.routes.find(_.url == result.endpointUrl)
        log.info(
          s"  Multi-route destination: looking for URL='${result.endpointUrl}', found=${matchingRoute.isDefined}"
        )
        matchingRoute.flatMap(_.revert)
      } else {
        log.warn(s"No endpoint URL recorded for multi-route destination: ${result.destination}")
        None
      }
    } else {
      // Single-route destination: use top-level revert
      log.info(s"  Single-route destination: hasRevert=${endpoint.revert.isDefined}")
      endpoint.revert
    }

  /** Publish the revert event with built endpoints. */
  private def publishRevertEvent(
      dlqEvent: DeadLetterEvent,
      revert: List[(HttpEndpointConfig, Option[JsValue])]
  ): Future[PublishResult] = {
    val (revertEndpoints, payloadPairs) = revert.map { case (endpoint, payload) =>
      (
        endpoint,
        endpoint.destinationName -> DestinationConfig(
          payload    = payload,
          pathParams = Map.empty
        )
      )
    }.unzip

    log.info(
      s"Publishing revert event with ${revertEndpoints.size} endpoints: ${revertEndpoints.map(_.destinationName).mkString(", ")}"
    )

    publisher.publishToEndpoints(
      OutboxEvent(
        aggregateId = dlqEvent.aggregateId,
        eventType   = s"${dlqEvent.eventType}:REVERT",
        payloads    = payloadPairs.toMap
      ),
      revertEndpoints
    )
  }

  private def buildRevertEndpointAndPayload(
      dlqEvent: DeadLetterEvent,
      result: AggregateResult,
      revertConfig: RevertConfig,
      originalEndpoint: HttpEndpointConfig
  ): Try[(HttpEndpointConfig, Option[JsValue])] = {
    val headers = originalEndpoint.headers ++ Map(
      "X-Revert-Aggregate-Id" -> dlqEvent.aggregateId,
      "X-Revert-Event-Type" -> dlqEvent.eventType,
      "X-Revert-Destination" -> result.destination
    )

    RevertEndpointBuilder.buildRevertEndpoint(
      revertConfig,
      result.requestPayload,
      result.responsePayload,
      s"${result.destination}.revert", // Use .revert suffix to match OrderCancelled
      headers,
      originalEndpoint.timeout
    )
  }

  /** Handles DLQ revert failures with retry logic.
    *
    * @param dlqEvent The DLQ event that failed
    * @param error Error message
    * @param retryAfter Optional retry time hint from the endpoint
    * @param isRetryable If false, immediately marks as FAILED without retry
    */
  private def handleRevertFailure(
      dlqEvent: DeadLetterEvent,
      error: String,
      retryAfter: Option[Instant] = None,
      isRetryable: Boolean        = true
  ): Future[Boolean] =
    if (!isRetryable) {
      log.error(s"DLQ event ${dlqEvent.id} failed with non-retryable error: $error")
      db.run(dlqRepo.markRevertFailed(dlqEvent.id, error)).map(_ => false)
    } else {
      val nextCount  = dlqEvent.revertRetryCount + 1
      val shouldFail = nextCount > maxRetries

      if (shouldFail) {
        log.error(
          s"DLQ event ${dlqEvent.id} exceeded revert retries (retry count: ${dlqEvent.revertRetryCount}, max: $maxRetries), marking as FAILED"
        )
        db.run(dlqRepo.markRevertFailed(dlqEvent.id, error)).map(_ => false)
      } else {
        val nextRetryAt = retryAfter.getOrElse {
          val backoffSeconds = Math.pow(2, nextCount).toLong
          Instant.now().plusSeconds(backoffSeconds)
        }

        log.info(
          s"DLQ event ${dlqEvent.id} will retry revert at $nextRetryAt (attempt $nextCount/$maxRetries)"
        )

        val action = for {
          _ <- dlqRepo.setRevertError(dlqEvent.id, error)
          _ <- dlqRepo.incrementRevertRetryCount(
            dlqEvent.id,
            dlqEvent.revertRetryCount,
            error,
            nextRetryAt
          )
        } yield false

        db.run(action)
      }
    }
}
