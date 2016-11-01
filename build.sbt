name := "SparkSecondarySort"

version := "1.0"

scalaVersion := "2.11.8"

mainClass in (Compile, run) := Some("com.demo.secondary.sort.SecondarySort")

mainClass in (Compile, packageBin) := Some("com.demo.secondary.sort.SecondarySort")

mainClass := Some("com.demo.secondary.sort.SecondarySort")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"
    