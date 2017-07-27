name := "SparkApps"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "1.1.0").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog").
    exclude("org.slf4j", "jcl-over-slf4j").
    excludeAll(
      ExclusionRule(organization = "org.eclipse.jetty.orbit")
    )
)

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11"
libraryDependencies += "com.google.code.gson" % "gson" % "2.3.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


