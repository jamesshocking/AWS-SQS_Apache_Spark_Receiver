name := "awssqs_spark_dstream"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("com.gastecka.awssqs")

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.1" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-sqs
libraryDependencies += "com.amazonaws" % "aws-java-sdk-sqs" % "1.11.1001"
