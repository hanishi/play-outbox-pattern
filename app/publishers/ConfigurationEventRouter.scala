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
  private val fanoutConfig: Map[String, List[String]] = loadFanoutConfig()

  override def routeEventToMultiple(event: OutboxEvent): List[HttpEndpointConfig] =
    fanoutConfig.get(event.eventType) match {
      case Some(destinations) =>
        // fanout is configured, resolve each destination to its endpoint config
        destinations.flatMap(destName =>
          routes.get(destName).map(_.copy(destinationName = destName))
        )
      case None =>
        // No fanout configured, use a single destination
        List(routeEvent(event).copy(destinationName = event.eventType))
    }

  override def routeEvent(event: OutboxEvent): HttpEndpointConfig =
    routes.getOrElse(event.eventType, defaultRoute)

  private def loadRoutes(): Map[String, HttpEndpointConfig] =
    config
      .getOptional[Configuration]("outbox.http.routes")
      .map { routesConfig =>
        routesConfig.subKeys.flatMap { eventType =>
          routesConfig.getOptional[Configuration](eventType).map { routeConfig =>
            loadMultiRoutes(routeConfig).fold {
              eventType -> HttpEndpointConfig(
                url = routeConfig.getOptional[String]("url"), 
                method = routeConfig.getOptional[String]("method").getOrElse("POST"),
                headers = routeConfig
                  .getOptional[Map[String, String]]("headers")
                  .getOrElse(Map.empty),
                timeout = Option(routeConfig.getOptional[FiniteDuration]("timeout")).flatten
                  .filter(_ != null)
                  .getOrElse(30.seconds),
                revert = loadRevertConfig(routeConfig),
                retryAfter = loadRetryAfterConfig(routeConfig),
                condition = loadConditionConfig(routeConfig)
              )
            } { routes =>
              eventType -> HttpEndpointConfig(
                url = None,
                method = routeConfig.getOptional[String]("method").getOrElse("POST"),
                headers = routeConfig
                  .getOptional[Map[String, String]]("headers")
                  .getOrElse(Map.empty),
                timeout = Option(routeConfig.getOptional[FiniteDuration]("timeout")).flatten
                  .filter(_ != null)
                  .getOrElse(30.seconds),
                revert = loadRevertConfig(routeConfig),
                retryAfter = loadRetryAfterConfig(routeConfig),
                condition = loadConditionConfig(routeConfig),
                routes = routes
              )
            }
          }
        }.toMap
      }
      .getOrElse(Map.empty)

  private def loadMultiRoutes(routeConfig: Configuration): Option[List[RouteOption]] =
    routeConfig.getOptional[Seq[Configuration]]("routes").map { routesSeq =>
      routesSeq.toList.flatMap { routeOptConfig =>
        for {
          url <- routeOptConfig.getOptional[String]("url")
          condition <- loadConditionConfig(routeOptConfig)
        } yield RouteOption(
          url       = url,
          condition = condition,
          revert    = loadRevertConfig(routeOptConfig)
        )
      }
    }

  private def loadRevertConfig(routeConfig: Configuration): Option[RevertConfig] =
    routeConfig.getOptional[Configuration]("revert").map { revertConfig =>
      RevertConfig(
        url     = revertConfig.get[String]("url"),
        method  = revertConfig.getOptional[String]("method").getOrElse("DELETE"),
        extract = revertConfig
          .getOptional[Map[String, String]]("extract")
          .getOrElse(Map.empty),
        payload = revertConfig.getOptional[String]("payload")
      )
    }

  private def loadConditionConfig(routeConfig: Configuration): Option[Condition] =
    routeConfig.getOptional[Configuration]("condition").flatMap { conditionConfig =>
      val operatorStr = conditionConfig.get[String]("operator")
      ComparisonOperator.fromString(operatorStr).map { op =>
        Condition(
          jsonPath            = conditionConfig.get[String]("jsonPath"),
          operator            = op,
          value               = conditionConfig.getOptional[String]("value"),
          previousDestination = conditionConfig.getOptional[String]("previousDestination")
        )
      }
    }

  private def loadRetryAfterConfig(routeConfig: Configuration): RetryAfterConfig =
    routeConfig.getOptional[Configuration]("retryAfter") match {
      case Some(retryConfig) =>
        val formatStr = retryConfig.getOptional[String]("format").getOrElse("auto")
        val format    = RetryAfterFormat.fromString(formatStr).getOrElse(RetryAfterFormat.Auto)

        RetryAfterConfig(
          headerName      = retryConfig.getOptional[String]("headerName").getOrElse("Retry-After"),
          format          = format,
          fromBody        = retryConfig.getOptional[Boolean]("fromBody").getOrElse(false),
          jsonPath        = retryConfig.getOptional[String]("jsonPath"),
          fallbackSeconds = retryConfig.getOptional[Long]("fallbackSeconds")
        )
      case None =>
        RetryAfterConfig() // Use defaults
    }

  private def loadDefaultRoute(): HttpEndpointConfig =
    HttpEndpointConfig(
      url    = config.getOptional[String]("outbox.http.defaultUrl"),
      method = config
        .getOptional[String]("outbox.http.defaultMethod")
        .getOrElse("POST"),
      headers = config
        .getOptional[Map[String, String]]("outbox.http.defaultHeaders")
        .getOrElse(Map.empty),
      timeout = Option(config.getOptional[FiniteDuration]("outbox.http.defaultTimeout")).flatten
        .filter(_ != null)
        .getOrElse(30.seconds)
    )

  private def loadFanoutConfig(): Map[String, List[String]] =
    config
      .getOptional[Configuration]("outbox.http.fanout")
      .map { fanoutConfig =>
        fanoutConfig.subKeys.flatMap { eventType =>
          fanoutConfig.getOptional[Seq[String]](eventType).map { destinations =>
            eventType -> destinations.toList
          }
        }.toMap
      }
      .getOrElse(Map.empty)
}
