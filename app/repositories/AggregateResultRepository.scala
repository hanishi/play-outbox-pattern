package repositories

import com.google.inject.{ Inject, Singleton }
import models.AggregateResult
import play.api.libs.json.*
import repositories.OutboxEventsPostgresProfile.api.*

import java.time.Instant
import scala.concurrent.ExecutionContext

enum Result:
  case Success, Failed, All

class AggregateResultTable(tag: Tag) extends Table[AggregateResult](tag, "aggregate_results") {
  import OutboxEventsPostgresProfile.OutboxEvents.playJsonTypeMapper

  def * = (
    id,
    aggregateId,
    eventType,
    destination,
    endpointUrl,
    httpMethod,
    requestPayload,
    responseStatus,
    responsePayload,
    publishedAt,
    durationMs,
    success,
    errorMessage
  ).mapTo[AggregateResult]

  def id              = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def aggregateId     = column[String]("aggregate_id")
  def eventType       = column[String]("event_type")
  def destination     = column[String]("destination")
  def endpointUrl     = column[String]("endpoint_url")
  def httpMethod      = column[String]("http_method")
  def requestPayload  = column[Option[JsValue]]("request_payload")
  def responseStatus  = column[Option[Int]]("response_status")
  def responsePayload = column[Option[JsValue]]("response_payload")
  def publishedAt     = column[Instant]("published_at")
  def durationMs      = column[Option[Int]]("duration_ms")
  def success         = column[Boolean]("success")
  def errorMessage    = column[Option[String]]("error_message")
}

@Singleton
class DestinationResultRepository @Inject() ()(using ec: ExecutionContext) {
  private val results = TableQuery[AggregateResultTable]

  /** Upserts (insert or update) an aggregate result.
    * If a result already exists for this aggregate+destination+event_type, it will be updated.
    * This ensures only the final result per aggregate+destination is stored.
    */
  def upsert(result: AggregateResult): DBIO[Int] = {

    val requestJson        = result.requestPayload.map(_.toString).getOrElse("null")
    val responseJson       = result.responsePayload.map(_.toString).getOrElse("null")
    val publishedTimestamp = java.sql.Timestamp.from(result.publishedAt)

    val upsertQuery = sqlu"""
      INSERT INTO aggregate_results (
        aggregate_id, event_type, destination, endpoint_url, http_method,
        request_payload, response_status, response_payload,
        published_at, duration_ms, success, error_message
      ) VALUES (
        ${result.aggregateId}, ${result.eventType}, ${result.destination}, ${result.endpointUrl},
        ${result.httpMethod}, $requestJson::jsonb, ${result.responseStatus},
        $responseJson::jsonb, $publishedTimestamp, ${result.durationMs},
        ${result.success}, ${result.errorMessage}
      )
      ON CONFLICT (aggregate_id, destination, event_type)
      DO UPDATE SET
        endpoint_url = EXCLUDED.endpoint_url,
        http_method = EXCLUDED.http_method,
        request_payload = EXCLUDED.request_payload,
        response_status = EXCLUDED.response_status,
        response_payload = EXCLUDED.response_payload,
        published_at = EXCLUDED.published_at,
        duration_ms = EXCLUDED.duration_ms,
        success = EXCLUDED.success,
        error_message = EXCLUDED.error_message
    """
    upsertQuery
  }

  /** Finds all results for a specific aggregate.
    * Returns them in reverse chronological order (newest first) for proper LIFO revert order.
    * This ensures compensating transactions execute in reverse: if the flow is A->B->C,
    * revert happens as C->B->A (Saga pattern).
    *
    * @param aggregateId The aggregate ID to filter by
    * @param filter Filter for success status: Success, Failed, or All
    * @param includeReverts Whether to include revert events (default: true for UI, false for DLQ processing)
    */
  def findByAggregateId(
                         aggregateId: String,
                         filter: Result = Result.All,
                         includeReverts: Boolean = true
  ): DBIO[Seq[AggregateResult]] = {
    val baseQuery = results.filter(_.aggregateId === aggregateId)

    // For DLQ revert processing, exclude revert events to only see forward flow results
    // For UI display, include both forward and revert events
    val revertFilteredQuery = if (includeReverts) baseQuery else baseQuery.filterNot(_.eventType like "%:REVERT")

    val filteredQuery = filter match {
      case Result.Success => revertFilteredQuery.filter(_.success === true)
      case Result.Failed  => revertFilteredQuery.filter(_.success === false)
      case Result.All     => revertFilteredQuery
    }
    filteredQuery.sortBy(_.publishedAt.desc).result
  }

  /** Finds all results for a specific destination. */
  def findByDestination(destination: String, limit: Int = 100): DBIO[Seq[AggregateResult]] =
    results
      .filter(_.destination === destination)
      .sortBy(_.publishedAt.desc)
      .take(limit)
      .result

  /** Finds all failed results (for debugging). */
  def findFailures(limit: Int = 100): DBIO[Seq[AggregateResult]] =
    results
      .filter(_.success === false)
      .sortBy(_.publishedAt.desc)
      .take(limit)
      .result

  /** Counts total results for an aggregate (useful for fan-out verification). */
  def countByAggregateId(aggregateId: String): DBIO[Int] =
    results
      .filter(_.aggregateId === aggregateId)
      .length
      .result

  /** Counts failures for an aggregate. */
  def countFailuresByAggregateId(aggregateId: String): DBIO[Int] =
    results
      .filter(result => result.aggregateId === aggregateId && result.success === false)
      .length
      .result
}
