lazy val root = (project in file(".")).settings(
  organization := "com.tresata",
  name := "spark-skewjoin",
  version := "0.6.0-SNAPSHOT",
  scalaVersion := "2.11.8",
  crossScalaVersions := Seq("2.10.6", "2.11.8"),
  javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.8", "-feature", "-language:_"),
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
  libraryDependencies ++= Seq(
    "com.novocode" % "junit-interface" % "0.11" % "test",
    "com.twitter" %% "algebird-core" % "0.13.0" % "compile",
    "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.3" % "test"
  ),
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
  publishArtifact in Test := false,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  credentials += Credentials(Path.userHome / ".m2" / "credentials_sonatype"),
  pomExtra := (
    <url>https://github.com/tresata/spark-skewjoin</url>
        <licenses>
      <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      <comments>A business-friendly OSS license</comments>
      </license>
      </licenses>
      <scm>
      <url>git@github.com:tresata/spark-skewjoin.git</url>
      <connection>scm:git:git@github.com:tresata/spark-skewjoin.git</connection>
      </scm>
      <developers>
      <developer>
      <id>koertkuipers</id>
      <name>Koert Kuipers</name>
      <url>https://github.com/koertkuipers</url>
        </developer>
      <developer>
      <id>yl2695</id>
      <name>Yucheng Lu</name>
      <url>https://github.com/yl2695</url>
        </developer>
      </developers>
  )
)

