package services

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ ActorSystem, DispatcherSelector }
import org.apache.pekko.stream.scaladsl.{ Keep, RestartSource, Source }
import org.apache.pekko.stream.{ KillSwitches, RestartSettings, UniqueKillSwitch }
import org.postgresql.PGConnection
import play.api.Logger
import slick.jdbc.JdbcBackend.Database

import java.sql.Connection
import scala.concurrent.duration.*
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.Exception.*
import scala.util.{ Try, Using }

object PostgresListenNotifyStream {

  private val logger = Logger(this.getClass)

  def createChannelSource(
      config: Config
  )(using db: Database, system: ActorSystem[?]): Source[ChannelNotification, UniqueKillSwitch] = {

    logger.info(s"Creating LISTEN/NOTIFY source for channels: ${config.channels.mkString(", ")}")

    given blockingEc: ExecutionContext =
      system.dispatchers.lookup(DispatcherSelector.fromConfig("blocking-jdbc"))

    def openConnection(): Connection = {
      val conn = db.source.createConnection()
      conn.setAutoCommit(true)
      config.channels.foreach { channel =>
        Using.resource(conn.createStatement())(_.execute(s"LISTEN $channel"))
        logger.debug(s"LISTEN connection opened for channel: $channel")
      }
      conn
    }

    def closeConnection(conn: Connection): Done = {
      Try(Using.resource(conn.createStatement())(_.execute("UNLISTEN *")))
      Try(conn.close())
      Done
    }

    val restartSettings = RestartSettings(
      config.restartMinBackoff,
      config.restartMaxBackoff,
      config.restartRandomFactor
    )

    RestartSource
      .withBackoff(restartSettings) { () =>
        Source
          .unfoldResourceAsync[Seq[ChannelNotification], Connection](
            create = () =>
              Future {
                catching(classOf[Throwable])
                  .withApply { ex =>
                    logger.error(s"Error opening LISTEN connection: $ex", ex)
                    throw ex
                  }
                  .apply(openConnection())
              }(blockingEc),
            read = conn =>
              Future {
                val pg            = conn.unwrap(classOf[PGConnection])
                val notifications =
                  Option(pg.getNotifications(config.pollIntervalMs)).toList.flatten

                val byChannel = notifications
                  .filter(n => config.channels.contains(n.getName))
                  .groupBy(_.getName)
                  .map { case (channel, notifs) =>
                    val eventIds = notifs.flatMap(n => Try(n.getParameter.toLong).toOption)
                    ChannelNotification(channel, eventIds)
                  }
                  .filter(_.eventIds.nonEmpty)
                  .toSeq

                if (byChannel.nonEmpty) {
                  byChannel.foreach { cn =>
                    logger.info(
                      s"LISTEN $cn.channel: got ${cn.eventIds.length} event IDs: ${cn.eventIds.mkString(", ")}"
                    )
                  }
                }

                Some(byChannel)
              }(blockingEc),
            close = conn => Future(closeConnection(conn))(blockingEc)
          )
          .mapConcat(identity)
      }
      .viaMat(KillSwitches.single)(Keep.right)
  }

  case class ChannelNotification(channel: String, eventIds: Seq[Long])

  case class Config(
      channels: Seq[String],
      pollIntervalMs: Int               = 200,
      restartMinBackoff: FiniteDuration = 500.millis,
      restartMaxBackoff: FiniteDuration = 30.seconds,
      restartRandomFactor: Double       = 0.2
  )
}
