import $ivy.`com.lihaoyi::mill-contrib-sonatypecentral:`
import mill.contrib.sonatypecentral.SonatypeCentralPublishModule

import coursier.maven.MavenRepository
import mill._, scalalib._, scalalib.publish._, scalafmt._
import mill.define.Cross.Resolver
import mill.scalalib.Assembly._
import $ivy.`com.lihaoyi::mill-contrib-buildinfo:`
import mill.contrib.buildinfo.BuildInfo
import $ivy.`io.chris-kipp::mill-ci-release::0.1.10`
import io.kipp.mill.ci.release.CiReleaseModule
import de.tobiasroeser.mill.vcs.version.VcsVersion

val url = "https://github.com/nightscape/spark-xml-backport"
object build extends Module {
  def publishVersion = T {
    VcsVersion.vcsState().format(untaggedSuffix = "-SNAPSHOT")
  }
}
trait SparkXmlBackportModule
    extends CrossScalaModule
    with Cross.Module2[String, String]
    with SbtModule
    with SonatypeCentralPublishModule
    with CiReleaseModule
    with ScalafmtModule {
  val sparkVersion = crossValue2
  val Array(sparkMajor, sparkMinor, sparkPatch) = sparkVersion.split("\\.")
  val sparkBinaryVersion = s"$sparkMajor.$sparkMinor"
  def millSourcePath = super.millSourcePath / os.up
  override def artifactNameParts: T[Seq[String]] =
    Seq("spark-xml-backport", sparkBinaryVersion)

  val sparkDeps = Agg(ivy"org.apache.spark::spark-core:$sparkVersion", ivy"org.apache.spark::spark-sql:$sparkVersion")
  def compileIvyDeps = sparkDeps ++
    Agg(ivy"org.glassfish.jaxb:txw2:2.2.11", ivy"org.apache.ws.xmlschema:xmlschema-core:2.3.1")
  def pomSettings = PomSettings(
    description = "A backport of the Spark 4 XML functionality. Supports streaming and partitioning.",
    organization = "dev.mauch.spark",
    url = url,
    licenses = Seq(License.MIT),
    versionControl = VersionControl(browsableRepository = Some(url), connection = Some(s"scm:git:$url.git")),
    developers = Seq(Developer("nightscape", "Martin Mauch", "https://github.com/nightscape"))
  )
  object test extends ScalaTests with SbtModuleTests with TestModule.ScalaTest {
    override def ivyDeps = Agg(
      ivy"org.scalatest::scalatest:3.2.19",
      ivy"org.scalatestplus::scalacheck-1-18:3.2.19.0",
      ivy"org.scalacheck::scalacheck:1.18.0",
      ivy"org.junit.jupiter:junit-jupiter:5.9.3"
    ) ++ SparkXmlBackportModule.this.compileIvyDeps()
  }
}

val scala213 = "2.13.13"
val scala212 = "2.12.19"
val spark33 = List("3.3.4")
val sparkVersions = spark33
val crossMatrix212 = sparkVersions.map(spark => (scala212, spark))
val crossMatrix213 = sparkVersions.filter(_ >= "3.2").map(spark => (scala213, spark))
val crossMatrix = crossMatrix212 ++ crossMatrix213

object root extends Cross[SparkXmlBackportModule](crossMatrix)
