ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "Joins"
  )

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0"
