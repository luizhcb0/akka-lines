name := "miv-horus-microservice-lines"

organization := "Banco do Brasil"

version := "0.0.1"

scalaVersion := "2.12.6"


credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

//Log Provided
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0" % "provided"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3" % "provided"
libraryDependencies += "net.debasishg" %% "redisclient" % "3.8"

// LOG Local
//libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
//libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

// TIBCO
//libraryDependencies += "com.tibco" % "tibrvnative" % "8.4"