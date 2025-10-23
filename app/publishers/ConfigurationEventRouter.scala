package publishers

import models.OutboxEvent
import play.api.Configuration

import javax.inject.*
import scala.concurrent.duration.*

@Singleton
class ConfigurationEventRouter @Inject() (
    config: Configuration
) extends EventRouter {

  private val routes: Map[String, HttpEndpointConfig] = loadRoutes()
  private val defaultRoute: HttpEndpointConfig        = loadDefaultRoute()

  override def routeEvent(event: OutboxEvent): HttpEndpointConfig =
    routes.getOrElse(event.eventType, defaultRoute)

  private def loadRoutes(): Map[String, HttpEndpointConfig] =
    config
      .getOptional[Configuration]("outbox.http.routes")
      .map { routesConfig =>
        routesConfig.subKeys.flatMap { eventType =>
          routesConfig.getOptional[Configuration](eventType).map { routeConfig =>
            eventType -> HttpEndpointConfig(
              url     = routeConfig.get[String]("url"),
              method  = routeConfig.getOptional[String]("method").getOrElse("POST"),
              headers = routeConfig
                .getOptional[Map[String, String]]("headers")
                .getOrElse(Map.empty),
              timeout = routeConfig
                .getOptional[FiniteDuration]("timeout")
                .getOrElse(30.seconds)
            )
          }
        }.toMap
      }
      .getOrElse(Map.empty)

  private def loadDefaultRoute(): HttpEndpointConfig =
    HttpEndpointConfig(
      url    = config.get[String]("outbox.http.defaultUrl"),
      method = config
        .getOptional[String]("outbox.http.defaultMethod")
        .getOrElse("POST"),
      headers = config
        .getOptional[Map[String, String]]("outbox.http.defaultHeaders")
        .getOrElse(Map.empty),
      timeout = config
        .getOptional[FiniteDuration]("outbox.http.defaultTimeout")
        .getOrElse(30.seconds)
    )
}
