ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "Joins"
  )

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"

libraryDependencies ++= Seq(
  // ScalaTest
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,

  // Spark testing base library
  "com.holdenkarau" %% "spark-testing-base" % "3.3.1_1.4.0" % Test,

  // Spark SQL test
  "org.apache.spark" %% "spark-sql" % "3.3.2" % Test,

  // Spark Hive dependency
  "org.apache.spark" %% "spark-hive" % "3.3.2" % Test
)
