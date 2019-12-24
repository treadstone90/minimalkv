resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  "Twitter Maven" at "https://maven.twttr.com"
)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
