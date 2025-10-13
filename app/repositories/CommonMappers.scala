package repositories

import models.{ DestinationConfig, EventStatus }
import play.api.libs.json.{ JsValue, Json }
import repositories.OutboxEventsPostgresProfile.api.*

object CommonMappers {
  import OutboxEventsPostgresProfile.OutboxEvents.playJsonTypeMapper
  import models.DestinationConfig.given

  given eventStatusMapper: BaseColumnType[EventStatus] =
    MappedColumnType.base[EventStatus, String](
      status => status.value,
      str => EventStatus.fromString(str)
    )

  given payloadsMapper: BaseColumnType[Map[String, DestinationConfig]] =
    MappedColumnType.base[Map[String, DestinationConfig], JsValue](
      map => Json.toJson(map), // Map[String, DestinationConfig] to JsValue for JSONB storage
      jsValue => jsValue.as[Map[String, DestinationConfig]] // JSONB back to Map
    )
}
