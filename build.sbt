name := "DataValidationFrameWork"
version := "1.0"
 
scalaVersion := "2.11.7"
 
libraryDependencies ++= Seq(
  "org.joda" % "joda-convert" % "1.7",
  "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided",
  "org.apache.spark" % "spark-hive_2.11" % "2.1.0" % "provided",
"org.apache.commons" % "commons-email" %"1.2",
  "org.scala-lang" % "scala-reflect" % "2.11.7",
  "com.typesafe" % "config" % "1.2.0",
  "org.scalatest" %% "scalatest" % "2.2.5",
  "com.github.nscala-time" %% "nscala-time" % "1.6.0"
)
 
resolvers ++= Seq(
  "conjars"  at "http://conjars.org/repo",
  "Bintray sbt plugin releases" at "http://dl.bintray.com/sbt/sbt-plugin-releases/",
  "Job Server Bintray" at "http://dl.bintray.com/spark-jobserver/maven",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)
 
 
scalacOptions ++= Seq("-unchecked", "-deprecation")
 
incOptions := incOptions.value.withNameHashing(true)
 
//assemblySettings
test in assembly := {}
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
