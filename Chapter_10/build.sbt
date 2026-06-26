name:= "spark-scala-app"
version:= "0.1.0"
scalaVersion:= "2.12.18"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.8" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.5.8" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.5.8" % "provided",
   "io.delta" %% "delta-spark" % "3.2.0"
)
