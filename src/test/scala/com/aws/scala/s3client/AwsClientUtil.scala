package com.aws.scala.s3client

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client, AmazonS3ClientBuilder}

import scala.collection.JavaConverters.asScalaBufferConverter

class AwsClientUtil(proxyUrl: String,
                    proxyPort: String,
                    bucketName: String,
                    fileName: String,
                    acccessKeyId: String,
                    secreteAccessKey: String
                    ) {
  def getClient(basicSessionCredentials: BasicSessionCredentials, clientConfiguration: ClientConfiguration): AmazonS3Client = {
    new AmazonS3Client(basicSessionCredentials, clientConfiguration)
  }

  def getClient(profileName: String, clientConfiguration: ClientConfiguration): AmazonS3 = {
    val credentialsProvider = new ProfileCredentialsProvider(profileName)
    val amazonS3ClientBuilder = AmazonS3ClientBuilder.standard
    amazonS3ClientBuilder.withClientConfiguration(clientConfiguration).withCredentials(credentialsProvider).withRegion("us-east-1").build()
  }

  def listObjectsWithSessionCredentials(sessionToken: String): List[String] = {
    val sessionCredentials = new BasicSessionCredentials(acccessKeyId, secreteAccessKey, sessionToken)
    val clientConfiguration = new ClientConfiguration
    clientConfiguration.setProxyHost(proxyUrl)
    clientConfiguration.setProxyPort(proxyPort.toInt)

    //val s3_client=getClient("profileName", clientConfiguration)
    //main client
    val s3Client = getClient(sessionCredentials, clientConfiguration)
    var final_list: List[String] = List()
    var list: List[String] = List()
    val request: ListObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(fileName)
    var result: ListObjectsV2Result = new ListObjectsV2Result()
    do {
      result = s3Client.listObjectsV2(request)
      println("Response from AWS: " + result)
      val token = result.getNextContinuationToken
      println("Next Continuation Token: " + token)
      request.setContinuationToken(token)
      list = (result.getObjectSummaries.asScala.map(_.getKey)).toList
      println(list)
      println(list.size)
      final_list = final_list ::: list
      println(final_list)
    } while (result.isTruncated)
    println("size", final_list.size)
    final_list
  }

  def listObjectsWithProfile(profileName: String): List[String] = {
    val clientConfiguration = new ClientConfiguration
    println("proxyUrl: " + proxyUrl)
    println("proxyPort: " + proxyPort)
    clientConfiguration.setProxyHost(proxyUrl)
    clientConfiguration.setProxyPort(proxyPort.toInt)

    getClient(profileName, clientConfiguration: ClientConfiguration)

    val s3Client = getClient(profileName, clientConfiguration)
    var final_list: List[String] = List()
    var list: List[String] = List()
    val request: ListObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(fileName)
    var result: ListObjectsV2Result = new ListObjectsV2Result()
    do {
      result = s3Client.listObjectsV2(request)
      println("Response from AWS: " + result)
      val token = result.getNextContinuationToken
      println("Next Continuation Token: " + token)
      request.setContinuationToken(token)
      list = (result.getObjectSummaries.asScala.map(_.getKey)).toList
      println(list)
      println(list.size)
      final_list = final_list ::: list
      println(final_list)
    } while (result.isTruncated)
    println("size", final_list.size)
    final_list
  }
}
