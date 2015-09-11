import Dependencies._

fork in run := true

javaOptions += "-Xms4g"

javaOptions += "-Xmx4g"

name := "common-core"

libraryDependencies ++= Seq(
  sprayJson,
  typesafeConfig,
  "redis.clients" % "jedis" % "2.7.2",
  Logging.logbackClassic,
  Logging.logbackCore,
  Logging.slf4jApi
)
