package publishers

import models.OutboxEvent

import scala.concurrent.duration.*

case class HttpEndpointConfig(
    url: String,
    method: String               = "POST",
    headers: Map[String, String] = Map.empty,
    timeout: FiniteDuration      = 30.seconds
)

trait EventRouter {
  def routeEvent(event: OutboxEvent): HttpEndpointConfig
}
