package com.gastecka.awssqs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, TimestampType}
import org.apache.spark.util.LongAccumulator

object AmazonSQSRecieverTest extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("aws-sqs-test")

  // We're going to process the records on the SQS queue in batches.  We'll stream
  // pausing for 5 seconds between fetch
  val batchInterval = Seconds(5)

  // How many batches to run before terminating.  Change this value to
  // run more or less batches
  val batchesToRun = 2

  // Message schema (for JSON objects).  An AWS SQS message is formatted as a JSON object.
  // Further reading on this can be found at
  // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_Message.html
  //
  // The Message attribute contains the data that is being shared by the producer
  val sqsSchema = StructType(
    StructField("Type", StringType, false) ::
    StructField("MessageId", StringType, false) ::
    StructField("TopicArn", StringType, false) ::
    StructField("Message", StringType, false) ::
    StructField("Timestamp", StringType, false) ::
    StructField("SignatureVersion", StringType, false) ::
    StructField("Signature", StringType, false) ::
    StructField("SigningCertURL", StringType, false) ::
    StructField("UnsubscribeURL", StringType, false) :: Nil
  )

  // The test assumes that the message producer is formatting their message using JSON
  // Assuming the same, change the schema to reflect the actual JSON format of the message
  val msgSchema = StructType(
    StructField("event", StructType(
      StructField("id", IntegerType, true) ::
      StructField("type", StringType, true) ::
      StructField("action", StringType, true) ::
      StructField("created_date", TimestampType, true) :: Nil
    ), false) :: Nil
  )

  // To control how many fetches/batches that we want to consume from the stream,
  // we're going to use the same accumulator singleton pattern as used by the Apache Spark team's own demos
  // https://github.com/apache/spark/blob/v3.1.2/examples/src/main/scala/org/apache/spark/examples/streaming/RecoverableNetworkWordCount.scala
  // and
  // https://spark.apache.org/docs/latest/streaming-programming-guide.html#accumulators-broadcast-variables-and-checkpoints
  object ReceivedBatchCounter {
    @volatile private var instance: LongAccumulator = null

    def getInstance(sc: SparkContext): LongAccumulator = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = sc.longAccumulator("ReceivedBatchCounter")
          }
        }
      }
      instance
    }
  }

  // declare your AWS SQS connection details, AWS access and secret key
  // region and the name of your queue
  val awsAccessKey = "<aws access key>"
  val awsSecretKey = "<aws secret key>"
  val awsRegion = "<aws region e.g. us-east-1>"
  val sqsQueueName = "<aws sqs name>"

  // a Spark structured streaming context
  val ssc = new StreamingContext(conf, batchInterval)

  val messages = ssc.receiverStream(new AmazonSQSReceiver(awsAccessKey, awsSecretKey, awsRegion, sqsQueueName))

  messages.foreachRDD { rdd =>
    // I want to use Spark Structured SQL - a lot easier to interact with the RDD
    val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    import spark.implicits._

    val rawDF = rdd.toDF("msg")
    // AWS SQS delivers messages structured to it's own JSON schema.  The receiver
    // instructs Spark to store the body of the received message (see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_Message.html)
    // of this message (I.e. store the body attribute's text
    // which is a text encoded JSON string, aligned to another JSON schema of Amazon's design.
    //
    // To make life easier, a Spark schema is applied.  At this point we can transform that actual
    // message payload, and then select just a single attribute from the JSON, just to
    // prove/demonstrate that it works
    val df = rawDF.select(from_json($"msg", sqsSchema) as "data")
      .select("data.*")
      .withColumn("message", from_json(col("Message"), msgSchema))
      .withColumn("Timestamp", to_timestamp(col("Timestamp")))
      .orderBy(col("Timestamp"))
      .select(col("Timestamp"), col("message.event.id"))

    df.cache()

    // now is the time to do something with the dataframe
    df.show()

    // https://spark.apache.org/docs/latest/streaming-programming-guide.html#accumulators-broadcast-variables-and-checkpoints
    val receivedBatchCounter = ReceivedBatchCounter.getInstance(ssc.sparkContext)

    println(s"--- Batch ${receivedBatchCounter.count + 1} ---")
    println("Processed messages in this batch: " + df.count())

    // Counting batches and terminating after 'batchesToRun'
    if (receivedBatchCounter.count >= batchesToRun - 1) {
      ssc.stop()
    } else {
      receivedBatchCounter.add(1)
    }
  }

  //
  ssc.start()
  ssc.awaitTermination()
}
