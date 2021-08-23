package com.gastecka.awssqs

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.auth.BasicAWSCredentials

class AWSBasicCredentialsProvider(awsAccessKey: String, awsSecretKey: String) extends AWSCredentialsProvider {

  override def getCredentials: AWSCredentials = {
    new BasicAWSCredentials(awsAccessKey, awsSecretKey)
  }

  override def refresh(): Unit = {}
}