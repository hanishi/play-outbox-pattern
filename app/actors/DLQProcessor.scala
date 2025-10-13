package actors

import com.jayway.jsonpath.{ JsonPath, Configuration as JsonPathConfig }
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

  private val log            = context.log
  private val jsonPathConfig = JsonPathConfig.defaultConfiguration()

  private val PlaceholderRegex = "\\{([A-Za-z0-9_.\\-]+)}".r

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
          s"DLQ Batch complete - Fetched: ${stats.fetched}, Reverted: ${stats.processed}, Failed: ${stats.failed}"
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
          log.info(s"  - ${result.destination}: ${result.endpointUrl} (success=${result.success})")
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
  ): Future[Boolean] = revertSuccessfulDestinations(
    dlqEvent,
    successfulResults,
    eventRouter.routeEventToMultiple(
      OutboxEvent(
        aggregateId = dlqEvent.aggregateId,
        eventType   = dlqEvent.eventType,
        payloads    = dlqEvent.payloads
      )
    )
  ).flatMap {
    case PublishResult.Success =>
      log.info(
        s"Successfully reverted all destinations for DLQ event ${dlqEvent.id} (aggregate: ${dlqEvent.aggregateId})"
      )
      db.run(dlqRepo.markProcessed(dlqEvent.id)).map(_ => true)

    case PublishResult.Retryable(error, retryAfter) =>
      log.warn(
        s"DLQ event ${dlqEvent.id} revert failed (retryable): $error, retry after: $retryAfter"
      )
      handleRevertRetryableFailure(dlqEvent, error, retryAfter)

    case PublishResult.NonRetryable(error) =>
      log.error(s"DLQ event ${dlqEvent.id} revert failed (non-retryable): $error")
      handleRevertNonRetryableFailure(dlqEvent, error)
  }

  /** Reverts successful destinations by building revert endpoints.
    * Uses fail-fast sequential publishing: if any revert fails, the entire
    * DLQ event fails and will be retried from the beginning in reverse order.
    */
  private def revertSuccessfulDestinations(
      dlqEvent: DeadLetterEvent,
      successfulResults: Seq[AggregateResult],
      endpoints: List[HttpEndpointConfig]
  ): Future[PublishResult] = {

    val endpointByName =
      endpoints
        .map(httpEndpointConfig => httpEndpointConfig.destinationName -> httpEndpointConfig)
        .toMap

    val revertData =
      successfulResults.flatMap { result =>
        endpointByName.get(result.destination) match {
          case None =>
            log.warn(s"No endpoint configuration found for destination: ${result.destination}")
            None
          case Some(endpoint) =>
            log.info(
              s"Building revert for ${result.destination}, routes.size=${endpoint.routes.size}, endpointUrl='${result.endpointUrl}'"
            )
            val revertConfigOpt =
              if (endpoint.routes.nonEmpty) {
                if (result.endpointUrl.nonEmpty) {
                  val matchingRoute = endpoint.routes.find(_.url == result.endpointUrl)
                  log.info(
                    s"  Multi-route destination: looking for URL='${result.endpointUrl}', found=${matchingRoute.isDefined}"
                  )
                  endpoint.routes.foreach { route =>
                    log.info(
                      s"    Available route: ${route.url}, hasRevert=${route.revert.isDefined}"
                    )
                  }
                  matchingRoute.flatMap(_.revert)
                } else {
                  log.warn(
                    s"No endpoint URL recorded for multi-route destination: ${result.destination}"
                  )
                  None
                }
              } else {
                log.info(s"  Single-route destination: hasRevert=${endpoint.revert.isDefined}")
                endpoint.revert
              }

            revertConfigOpt match {
              case Some(revertConfig) =>
                log.info(s"  Found revert config for ${result.destination}")
                buildRevertEndpointAndPayload(dlqEvent, result, revertConfig, endpoint) match {
                  case Success((revertEndpoint, payload)) =>
                    revertEndpoint.url.foreach(url =>
                      log.info(
                        s"  Successfully built revert endpoint for ${result.destination}: $url"
                      )
                    )
                    Some((revertEndpoint, payload))
                  case Failure(ex) =>
                    log.error(
                      s"Failed to build revert endpoint for ${result.destination}: ${ex.getMessage}"
                    )
                    None
                }
              case None =>
                log.warn(s"  No revert config found for ${result.destination}")
                None
            }
        }
      }.toList

    if (revertData.isEmpty) {
      log.info(s"No revert endpoints to execute for aggregate ${dlqEvent.aggregateId}")
      Future.successful(PublishResult.Success)
    } else {
      val (revertEndpoints, payloadPairs) = revertData.map { case (endpoint, payload) =>
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
  }

  private def buildRevertEndpointAndPayload(
      dlqEvent: DeadLetterEvent,
      result: AggregateResult,
      revertConfig: RevertConfig,
      originalEndpoint: HttpEndpointConfig
  ): Try[(HttpEndpointConfig, Option[JsValue])] =
    for {
      placeholderValues <- extractPlaceholderValues(
        revertConfig,
        result.requestPayload,
        result.responsePayload
      )
      revertUrl <- buildRevertUrl(revertConfig.url, placeholderValues)
    } yield (
      HttpEndpointConfig(
        destinationName = result.destination,
        url             = Some(revertUrl),
        method          = revertConfig.method,
        headers         = originalEndpoint.headers ++ Map(
          "X-Revert-Aggregate-Id" -> dlqEvent.aggregateId,
          "X-Revert-Event-Type" -> dlqEvent.eventType,
          "X-Revert-Destination" -> result.destination
        ),
        timeout = originalEndpoint.timeout,
        revert  = None // Reverts don't have nested reverts
      ),
      buildRevertPayload(revertConfig, placeholderValues)
    )

  /** Builds revert payload from the template. If no template is provided, returns None. */
  private def buildRevertPayload(
      revertConfig: RevertConfig,
      placeholderValues: Map[String, String]
  ): Option[JsValue] = revertConfig.payload.flatMap { template =>
    Try(Json.parse(placeholderValues.replacePlaceholders(template))).toOption
  }

  /** Extracts placeholder values from request and/or response payloads using JSONPath.
    * Supports prefix notation:
    * - "request:$.amount" - extract from request payload
    * - "response:$.id"    - extract from response payload
    * - "$.id"             - defaults to response payload for backward compatibility
    *
    * This implementation avoids repeated JSON parsing by pre-parsing each payload once
    * and uses a Map builder to minimize intermediate allocations.
    */
  private def extractPlaceholderValues(
      revertConfig: RevertConfig,
      requestPayload: Option[JsValue],
      responsePayload: Option[JsValue]
  ): Try[Map[String, String]] = Try {
    // Pre-stringify and pre-parse once per source to avoid repeated work per placeholder
    val requestStrOpt  = requestPayload.map(_.toString())
    val responseStrOpt = responsePayload.map(_.toString())

    val requestCtxOpt  = requestStrOpt.map(s => JsonPath.using(jsonPathConfig).parse(s))
    val responseCtxOpt = responseStrOpt.map(s => JsonPath.using(jsonPathConfig).parse(s))

    // Fast-path: nothing to extract
    if (revertConfig.extract.isEmpty) Map.empty
    else {
      val builder = Map.newBuilder[String, String]

      // Local helpers
      def parseSpec(spec: String): (String, String) = {
        val idx = spec.indexOf(':')
        if (idx > 0) {
          spec.substring(0, idx) match {
            case "request" => ("request", spec.substring(idx + 1))
            case "response" => ("response", spec.substring(idx + 1))
            case _ => ("response", spec) // Unknown prefix → fallback to response
          }
        } else ("response", spec)
      }

      def ctxFor(source: String) = source match {
        case "request" =>
          requestCtxOpt.getOrElse(
            throw new IllegalArgumentException(
              s"Cannot extract from request payload: request payload is not available"
            )
          )
        case "response" =>
          responseCtxOpt.getOrElse(
            throw new IllegalArgumentException(
              s"Cannot extract from response payload: response payload is not available"
            )
          )
      }

      revertConfig.extract.foreach { case (placeholderName, pathSpec) =>
        val (source, jsonPathExpr) = parseSpec(pathSpec)
        val ctx                    = ctxFor(source)

        val valueAny = Try(ctx.read[Any](jsonPathExpr)).getOrElse {
          throw new IllegalArgumentException(
            s"JSONPath expression '$jsonPathExpr' did not match any value in $source payload"
          )
        }

        builder += placeholderName -> valueAny.toString
      }

      builder.result()
    }
  }

  /** Builds the revert URL by replacing placeholders with extracted values.
    * - Fails with error if any placeholders are missing
    */
  private def buildRevertUrl(
      urlTemplate: String,
      placeholderValues: Map[String, String]
  ): Try[String] = Try {
    val placeholdersInTemplate = PlaceholderRegex.findAllMatchIn(urlTemplate).map(_.group(1)).toSet

    if (placeholdersInTemplate.isEmpty) urlTemplate
    else {
      val missing = placeholdersInTemplate.diff(placeholderValues.keySet)
      if (missing.nonEmpty)
        throw new IllegalArgumentException(
          s"Missing placeholders: ${missing.toList.sorted.mkString(", ")} in URL template: $urlTemplate"
        )

      val unused = placeholderValues.keySet.diff(placeholdersInTemplate)
      if (unused.nonEmpty)
        log.debug(
          s"Ignoring unused revert placeholders: ${unused.toList.sorted.mkString(", ")} in URL template: $urlTemplate"
        )

      placeholderValues.replacePlaceholders(urlTemplate)
    }
  }

  private def handleRevertRetryableFailure(
      dlqEvent: DeadLetterEvent,
      error: String,
      retryAfter: Option[Instant]
  ): Future[Boolean] = {
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

  private def handleRevertNonRetryableFailure(
      dlqEvent: DeadLetterEvent,
      error: String
  ): Future[Boolean] =
    db.run(dlqRepo.markRevertFailed(dlqEvent.id, error)).map(_ => false)

  private def handleRevertFailure(dlqEvent: DeadLetterEvent, error: String): Future[Boolean] = {
    val nextCount  = dlqEvent.revertRetryCount + 1
    val shouldFail = nextCount > maxRetries

    if (shouldFail) {
      log.error(
        s"DLQ event ${dlqEvent.id} exceeded revert retries (retry count: ${dlqEvent.revertRetryCount}, max: $maxRetries), marking as FAILED"
      )
      db.run(dlqRepo.markRevertFailed(dlqEvent.id, error)).map(_ => false)
    } else {
      val backoffSeconds = Math.pow(2, nextCount).toLong
      val nextRetryAt    = Instant.now().plusSeconds(backoffSeconds)

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
