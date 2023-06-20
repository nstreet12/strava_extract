ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"
name := "StravaExtract"
version := "1.0"
val sparkVersion = "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "strava_extract"
  )

libraryDependencies ++= Seq("org.scalaj" % "scalaj-http_2.12" % "2.4.2",
                            "org.apache.spark" %% "spark-core" % sparkVersion,
                            "org.apache.spark" %% "spark-sql" % sparkVersion,
                            "org.apache.spark" %% "spark-mllib" % sparkVersion,
                            "com.lihaoyi" %% "upickle" % "0.7.1",
                            "org.scalikejdbc" %% "scalikejdbc" % "2.5.2",
                            "com.github.kiambogo" %% "scrava" % "1.3.0",
                            "net.liftweb" %% "lift-json" % "3.0.1",
                            "org.mariadb.jdbc" % "mariadb-java-client" % "2.7.4",
                            "com.typesafe" % "config" % "1.4.1")