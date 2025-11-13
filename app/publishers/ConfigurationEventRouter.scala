package publishers

import models.OutboxEvent
import play.api.Configuration

import javax.inject.*
import scala.concurrent.duration.*

/** Configuration-based event router that maps events to HTTP endpoints.
  *
  * == Purpose ==
  * Routes events to external API endpoints based on application.conf configuration.
  * Supports fan-out (single event → multiple APIs), conditional routing, and automatic
  * revert destination generation for Saga compensation.
  *
  * == Configuration Format ==
  *
  * '''Simple Routing:'''
  * {{{
  * outbox.http.routes {
  *   OrderCreated {
  *     url = "http://billing/charge"
  *     method = "POST"
  *   }
  * }
  * }}}
  *
  * '''Fan-Out Routing:'''
  * {{{
  * outbox.http.fanout {
  *   OrderCreated = ["inventory", "fraudCheck", "shipping", "billing"]
  * }
  * outbox.http.routes {
  *   inventory { url = "http://localhost:9000/api/inventory/reserve" }
  *   fraudCheck { url = "http://localhost:9000/api/fraud/check" }
  *   shipping { url = "http://localhost:9000/api/domestic-shipping" }
  *   billing { url = "http://localhost:9000/api/billing" }
  * }
  * }}}
  *
  * '''Revert Configuration (for Saga compensation):'''
  * {{{
  * billing {
  *   url = "http://localhost:9000/api/billing"
  *   revert {
  *     url = "http://localhost:9000/api/billing/{transactionId}/refund"
  *     method = "POST"
  *     extract {
  *       transactionId = "response:$.transaction.id"
  *       amount = "request:$.amount"
  *       currency = "request:$.currency"
  *     }
  *     payload = """{"transactionId": "{transactionId}", "amount": "{amount}", "currency": "{currency}", "reason": "order_failed"}"""
  *   }
  * }
  * }}}
  *
  * == Routing Logic ==
  *
  * '''Forward Events (e.g., OrderCreated):'''
  * 1. Lookup fanout config: OrderCreated → ["inventory", "fraudCheck", "shipping", "billing"]
  * 2. For each destination, create Forward(endpoint)
  * 3. Returns: [Forward(inventory), Forward(fraudCheck), Forward(shipping), Forward(billing)]
  *
  * '''Revert Events (e.g., !OrderCreated):'''
  * 1. Strip ! prefix: !OrderCreated → OrderCreated
  * 2. Lookup fanout config: OrderCreated → ["inventory", "fraudCheck", "shipping", "billing"]
  * 3. '''Reverse order''' (LIFO): ["billing", "shipping", "fraudCheck", "inventory"]
  * 4. For each destination, create Revert(destinationName)
  * 5. Returns: [Revert("billing"), Revert("shipping"), Revert("fraudCheck"), Revert("inventory")]
  * 6. Runtime will gracefully skip destinations without revert configs (e.g., fraudCheck is read-only)
  *
  * == Multi-Route Destinations ==
  *
  * Destinations can have multiple routes with conditions (evaluated at runtime):
  * {{{
  * # Example 1: Routing based on request payload
  * shipping {
  *   method = "POST"
  *   routes = [
  *     {
  *       url = "http://localhost:9000/api/domestic-shipping"
  *       condition { jsonPath = "$.shippingType", operator = "eq", value = "domestic" }
  *     },
  *     {
  *       url = "http://localhost:9000/api/international-shipping"
  *       condition { jsonPath = "$.shippingType", operator = "eq", value = "international" }
  *     }
  *   ]
  * }
  *
  * # Example 2: Routing based on previous destination response
  * billing {
  *   method = "POST"
  *   routes = [
  *     {
  *       url = "http://localhost:9000/api/billing"
  *       condition { jsonPath = "$.riskScore", operator = "lt", value = "50", previousDestination = "fraudCheck" }
  *     },
  *     {
  *       url = "http://localhost:9000/api/high-value-processing"
  *       condition { jsonPath = "$.riskScore", operator = "gte", value = "50", previousDestination = "fraudCheck" }
  *     }
  *   ]
  * }
  * }}}
  *
  * Each route can have its own revert config matched by forward URL.
  *
  * == Key Features ==
  *
  * '''LIFO Revert Order:'''
  * - Automatically reverses fanout order for revert events
  * - Ensures proper Saga compensation (last-in-first-out)
  * - Critical for maintaining system consistency
  *
  * '''Graceful Revert Handling:'''
  * - Includes all destinations in revert list (no pre-filtering)
  * - Runtime skips destinations without revert configs
  * - Allows partial revertability (e.g., fraudCheck is read-only, has no revert)
  *
  * '''Startup Validation:'''
  * - Validates all fanout destination references exist in routes
  * - Prevents runtime errors from misconfiguration
  * - Fails fast at application startup
  */
@Singleton
class ConfigurationEventRouter @Inject() (
    config: Configuration
) extends EventRouter {

  private val routes: Map[String, HttpEndpointConfig] = loadRoutes()
  private val fanoutConfig: Map[String, List[String]] = loadFanoutConfig()

  validateFanoutReferences()

  /** Routes an event to destinations based on configuration.
    *
    * For forward events (OrderCreated), returns Forward destinations in configured order.
    * For revert events (!OrderCreated), returns Revert destinations in reversed order (LIFO).
    *
    * The runtime (HttpEventPublisher) will handle:
    * - For Forward: Conditional routing, payload validation, API calls
    * - For Revert: Query DB, build revert endpoints, call revert APIs, gracefully skip if no revert config
    */
  override def routeEventToDestinations(event: OutboxEvent): List[RoutedDestination] = {
    val isRevert = event.eventType.startsWith("!")

    (isRevert, fanoutConfig.get(if (isRevert) event.eventType.drop(1) else event.eventType)) match {
      case (true, Some(forwardDestNames)) =>
        // Revert event with fanout: reverse for LIFO and create Revert destinations
        // Runtime will gracefully skip destinations without revert configs
        for {
          destName <- forwardDestNames.reverse
          endpoint <- routes.get(destName)
        } yield RoutedDestination.Revert(baseDestination = destName)

      case (true, None) =>
        // Revert event without fanout configuration
        List.empty

      case (false, Some(destNames)) =>
        // Forward event with fanout: resolve each destination to Forward
        for {
          destName <- destNames
          endpoint <- routes.get(destName)
        } yield RoutedDestination.Forward(endpoint.copy(destinationName = destName))

      case (false, None) =>
        // Forward event without fanout: use a single destination if route exists
        routes.get(event.eventType) match {
          case Some(endpoint) => List(RoutedDestination.Forward(endpoint.copy(destinationName = event.eventType)))
          case None           => List.empty // No route configured for this event type
        }
    }
  }

  override def routeEvent(event: OutboxEvent): HttpEndpointConfig =
    routes.getOrElse(
      event.eventType,
      throw new IllegalStateException(
        s"No route configured for event type '${event.eventType}'. " +
          s"Available routes: ${routes.keys.mkString(", ")}"
      )
    )

  /** Gets the configured fanout order for an event type.
    * For revert events (prefixed with !), returns the reversed fanout of the base event with .revert suffix.
    * Runtime will gracefully skip destinations without revert configs.
    */
  def getFanoutOrder(eventType: String): Option[List[String]] =
    if (eventType.startsWith("!")) {
      val baseEventType = eventType.drop(1)
      fanoutConfig.get(baseEventType).map { forwardOrder =>
        forwardOrder.reverse
          .map(destName => s"$destName.revert")
      }
    } else {
      fanoutConfig.get(eventType)
    }

  override def findRevertConfig(baseDestination: String, forwardUrl: String): Option[RevertConfig] =
    routes.get(baseDestination).flatMap { endpoint =>
      if (endpoint.routes.isEmpty)
        endpoint.revert // Single-route: use top-level revert config
      else
        endpoint.routes.find(_.url == forwardUrl).flatMap(_.revert) // Multi-route: match by URL
    }

  private def loadRoutes(): Map[String, HttpEndpointConfig] =
    config
      .getOptional[Configuration]("outbox.http.routes")
      .map { routesConfig =>
        routesConfig.subKeys.flatMap { eventType =>
          routesConfig.getOptional[Configuration](eventType).map { routeConfig =>
            loadMultiRoutes(routeConfig).fold {
              eventType -> HttpEndpointConfig(
                url     = routeConfig.getOptional[String]("url"),
                method  = routeConfig.getOptional[String]("method").getOrElse("POST"),
                headers = routeConfig
                  .getOptional[Map[String, String]]("headers")
                  .getOrElse(Map.empty),
                timeout = Option(routeConfig.getOptional[FiniteDuration]("timeout")).flatten
                  .filter(_ != null)
                  .getOrElse(30.seconds),
                revert     = loadRevertConfig(routeConfig),
                retryAfter = loadRetryAfterConfig(routeConfig),
                condition  = loadConditionConfig(routeConfig)
              )
            } { routes =>
              eventType -> HttpEndpointConfig(
                url     = None,
                method  = routeConfig.getOptional[String]("method").getOrElse("POST"),
                headers = routeConfig
                  .getOptional[Map[String, String]]("headers")
                  .getOrElse(Map.empty),
                timeout = Option(routeConfig.getOptional[FiniteDuration]("timeout")).flatten
                  .filter(_ != null)
                  .getOrElse(30.seconds),
                revert     = loadRevertConfig(routeConfig),
                retryAfter = loadRetryAfterConfig(routeConfig),
                condition  = loadConditionConfig(routeConfig),
                routes     = routes
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

  /** Validates that all fanout destination references to exist in routes.
    */
  private def validateFanoutReferences(): Unit =
    fanoutConfig.foreach { case (eventType, destNames) =>
      destNames.foreach { destName =>
        if (!routes.contains(destName)) {
          throw new IllegalStateException(
            s"Fanout configuration for '$eventType' references unknown route '$destName'. " +
              s"Available routes: ${routes.keys.mkString(", ")}"
          )
        }
      }
    }
}
