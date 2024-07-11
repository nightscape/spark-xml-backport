inThisBuild(
  List(
    scalaVersion := "2.12.19",
    organization := "dev.mauch",
    githubWorkflowTargetTags ++= Seq("v*"),
    githubWorkflowPublish := Seq(
      WorkflowStep.Sbt(
        commands = List("ci-release"),
        name = Some("Publish project"),
        env = Map(
          "PGP_PASSPHRASE" -> "${{ secrets.PGP_PASSPHRASE }}",
          "PGP_SECRET" -> "${{ secrets.PGP_SECRET }}",
          "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
          "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}"
        )
      )
    ),
    githubWorkflowPublishTargetBranches := Seq(RefPredicate.Equals(Ref.Branch("main"))),
    githubWorkflowBuildMatrixAdditions := Map("spark-version" -> List("3.3.2")),
    githubWorkflowBuildSbtStepPreamble := Seq("++${{ matrix.scala }}", "-Dspark.version=${{ matrix.spark-version }}"),
    homepage := Some(url("https://github.com/nightscape/spark-xml-backport")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(Developer("nightscape", "Martin Mauch", "martin@mauch.dev", url("https://mauch.dev"))),
    sonatypeCredentialHost := "s01.oss.sonatype.org"
  )
)
val sparkVersion = sys.props.getOrElse("spark.version", "3.3.2")
val sparkDeps = Seq("org.apache.spark" %% "spark-sql" % sparkVersion)

lazy val xmlBackport = project
  .in(file("."))
  .settings(
    name := "spark-xml-backport",
    version := sparkVersion,
    libraryDependencies ++= sparkDeps ++ Seq(
      "org.glassfish.jaxb" % "txw2" % "2.2.11",
      "org.apache.ws.xmlschema" % "xmlschema-core" % "2.3.1",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test,
      "org.scalacheck" %% "scalacheck" % "1.18.0" % Test,
      "org.junit.jupiter" % "junit-jupiter" % "5.9.3" % Test
    ),
    Test / fork := true,
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local"
  )
