organization := "com.treadstone90"
name := "minimalkv"
version := "0.0.1-SNAPSHOT"

libraryDependencies += "com.twitter" %% "twitter-server" % "19.11.0"
libraryDependencies += "com.twitter" %% "util-core" % "19.11.0"
libraryDependencies += "com.twitter" % "util_2.12" % "7.0.0"
libraryDependencies += "com.google.guava" % "guava" % "27.1-jre"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"

libraryDependencies ++= Seq(
  "com.twitter" %% "finatra-jackson" % "19.11.0" % "test" classifier "tests",
  "com.twitter" %% "finatra-http" % "19.11.0",
  "com.twitter" %% "inject-server" % "19.11.0" % "test" classifier "tests",
  "com.twitter" %% "inject-app" % "19.11.0" % "test" classifier "tests",
  "com.twitter" %% "inject-core" % "19.11.0" % "test" classifier "tests",
  "com.twitter" %% "inject-modules" % "19.11.0" % "test" classifier "tests"
)
libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test"
libraryDependencies += "org.mockito" % "mockito-scala_2.12" % "1.10.1" % "test"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}