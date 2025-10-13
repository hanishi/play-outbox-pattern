package modules

import com.google.inject.{ AbstractModule, Provides }
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import play.api.db.slick.*
import play.api.{ Configuration, Environment }
import publishers.{ ConfigurationEventRouter, EventPublisher, EventRouter, HttpEventPublisher }
import slick.jdbc.*
import slick.jdbc.JdbcBackend.Database

import javax.inject.Singleton

class OutboxModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[EventRouter])
      .to(classOf[ConfigurationEventRouter])

    val publisherType = configuration
      .getOptional[String]("outbox.publisher.type")
      .getOrElse("http")

    publisherType match {
      case "http" =>
        bind(classOf[EventPublisher])
          .to(classOf[HttpEventPublisher])

      case other =>
        throw new IllegalArgumentException(
          s"Unknown publisher type: $other. Supported types: http"
        )
    }

    val autoStart = configuration.getOptional[Boolean]("outbox.autoStart").getOrElse(true)
    if (autoStart) {
      bind(classOf[services.EventProcessingService]).asEagerSingleton()
      bind(classOf[services.NotificationService]).asEagerSingleton()
    }
  }

  @Provides
  @Singleton
  def provideActorSystem(
      lifecycle: play.api.inject.ApplicationLifecycle
  ): ActorSystem[Nothing] = {
    val system = ActorSystem[Nothing](Behaviors.empty, "outbox-system")

    lifecycle.addStopHook { () =>
      system.terminate()
      system.whenTerminated.map(_ => ())(scala.concurrent.ExecutionContext.global)
    }
    system
  }

  @Provides
  @Singleton
  def provideDatabase(dbConfigProvider: DatabaseConfigProvider): Database =
    dbConfigProvider.get[PostgresProfile].db.asInstanceOf[Database]
}
