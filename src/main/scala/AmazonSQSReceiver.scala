package com.gastecka.awssqs

import com.amazonaws.ClientConfiguration
import org.apache.spark.internal.Logging
import scala.util.control.NonFatal
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import scala.collection.JavaConverters._

class AmazonSQSReceiver(awsAccessKey: String, awsSecretKey: String, awsRegion: String, queueName: String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  private val maxConnections: Int = 1
  private val maxNumMessages: Int = 10
  var sqsClient: AmazonSQS = _

  def onStart() {
    // Start the thread that receives from data AWS SQS
    // This code follows the template outlined at
    // https://spark.apache.org/docs/latest/streaming-custom-receivers.html
    new Thread("AMZ SQS Receiver") {
      setDaemon(true)
      override def run(): Unit = {

        sqsClient = AmazonSQSClientBuilder
          .standard()
          .withClientConfiguration(new ClientConfiguration().withMaxConnections(maxConnections))
          .withCredentials(new AWSBasicCredentialsProvider(awsAccessKey, awsSecretKey))
          .withRegion(awsRegion)
          .build()

        val queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl
        logInfo(s"SQS Queue Url: $queueUrl")

        receive(queueUrl)
      }
    }.start()
  }

  def onStop() {
    // in case restart thread close it twice
    synchronized {
      if (sqsClient != null) {
        sqsClient.shutdown()
        logInfo(s"Closed sqsClient: queue [$queueName]")
      }
    }
  }

  def receive(sqsQueueUrl: String): Unit ={
    try {

      val messages = sqsClient.receiveMessage(new ReceiveMessageRequest()
        .withQueueUrl(sqsQueueUrl)
        .withMaxNumberOfMessages(maxNumMessages))
        .getMessages.asScala


      // use store(multi-records) to get the best reliability
      // See https://spark.apache.org/docs/2.1.1/streaming-custom-receivers.html,
      //     section on Reciever Reliability
      if(!isStopped()) {
        // save the received messages read for Spark to process
        for(message <- messages) {
          store(message.getBody)

          // now ask SQS to delete the received message
          sqsClient.deleteMessage(new DeleteMessageRequest()
            .withQueueUrl(sqsQueueUrl)
            .withReceiptHandle(message.getReceiptHandle))
        }
      }

      if (!isStopped()) {
        restart("SQS data stream had no more data")
      } else {
        logInfo("Stopped receiving")
      }
    }
    catch {
      case NonFatal(e) =>
        logWarning("Error receiving data", e)
        restart("Error receiving data", e)
    } finally {
      onStop()
    }
  }
}
