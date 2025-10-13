package models

import play.api.libs.json.JsValue

/** Common trait for events that belong to an aggregate.
  *
  * Provides common identity and payload access across both outbox events
  * and DLQ events.
  */
trait AggregateEvent {
  def aggregateId: String
  def eventType: String
  def payloads: Map[String, DestinationConfig]

  /** Gets the payload for a specific destination.
    * Returns None if no payload exists for that destination or if the payload is None.
    */
  def getPayloadFor(destinationName: String): Option[JsValue] =
    payloads.get(destinationName).flatMap(_.payload)

  /** Gets the path parameters for a specific destination.
    * Returns empty map if no destination exists.
    */
  def getPathParamsFor(destinationName: String): Map[String, String] =
    payloads.get(destinationName).map(_.pathParams).getOrElse(Map.empty)

  /** Returns all destination names configured for this event */
  def destinationNames: Set[String] = payloads.keySet

  /** Check if this event has a payload for the given destination */
  def hasDestination(destinationName: String): Boolean =
    payloads.contains(destinationName)
}
