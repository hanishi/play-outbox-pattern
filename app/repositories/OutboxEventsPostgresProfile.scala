package repositories

import com.github.tminglei.slickpg.*

/** Custom PostgreSQL profile with JSONB support using slick-pg.
  */
trait OutboxEventsPostgresProfile extends ExPostgresProfile with PgPlayJsonSupport {

  override val pgjson = "jsonb"

  override val api: OutboxEvents.type = OutboxEvents

  object OutboxEvents extends ExtPostgresAPI with JsonImplicits
}

object OutboxEventsPostgresProfile extends OutboxEventsPostgresProfile
