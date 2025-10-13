name := "play-outbox-pattern"
version := "1.0.0"
scalaVersion := "3.3.6"


lazy val root = (project in file("."))
  .enablePlugins(PlayScala)

libraryDependencies ++= Seq(
  guice,
  "com.typesafe.slick" %% "slick" % "3.6.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.6.1",
  "org.playframework" %% "play-slick" % "6.2.0",
  "org.postgresql" % "postgresql" % "42.7.8",
  "com.typesafe.play" %% "play-json" % "2.10.8",
  ws,
  "org.scalatestplus.play" %% "scalatestplus-play" % "7.0.2" % Test,
  "org.apache.pekko" %% "pekko-actor-testkit-typed" % "1.2.1" % Test,
)

PlayKeys.playDefaultPort := 9000
