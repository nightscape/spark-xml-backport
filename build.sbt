ThisBuild / scalaVersion := "2.12.19"
ThisBuild / organization := "dev.mauch"
val sparkVersion = sys.props.getOrElse("spark.version", "3.3.2")
val sparkDeps = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

lazy val xmlBackport = project
  .in(file("."))
  .settings(
    name := "spark-xml-backport",
    version := sparkVersion,
    libraryDependencies ++= sparkDeps ++ Seq(
    "org.glassfish.jaxb" % "txw2" % "2.2.11",
    "org.apache.ws.xmlschema" % "xmlschema-core" % "2.3.1",
    )
  )
