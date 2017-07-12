name := "SampleSparkStream" 
version := "0.1" 
scalaVersion := "2.10.6"
val sparkVersion = "1.6.2"
//resolvers += "DataStax Repo" at "https://datastax.artifactoryonline.com/datastax/public-repos/" 
val dseVersion = "5.0.2" 
resolvers += "Apache Staging" at "https://repository.apache.org/content/groups/staging/"
libraryDependencies ++= Seq(
  // The excludes of jms, jmxtools and jmxri are required as per https://issues.apache.org/jira/browse/KAFKA-974.
  // The exclude of slf4j-simple is because it overlaps with our use of logback with slf4j facade;  without the exclude
  // we get slf4j warnings and logback's configuration is not picked up.
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.1"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
    exclude("org.slf4j", "slf4j-simple"),
  // Logback with slf4j facade
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "ch.qos.logback" % "logback-core" % "1.0.13",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-assembly" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion
  //"com.typesafe.play" %% "play-json" % "2.4.6"
)