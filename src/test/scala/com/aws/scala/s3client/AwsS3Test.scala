package com.aws.scala.s3client

import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap, FunSuite}
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, S3ObjectSummary}

import scala.collection.JavaConverters.asScalaBufferConverter

class AwsS3Test extends FunSuite with BeforeAndAfterAllConfigMap {
  var clientId = ""
  var clientSecret = ""
  var loginURL = ""
  var awsTokenUrl = ""
  var bucketName = ""
  var fileName = ""
  var datasetid1 = ""

   override def beforeAll(configMap: ConfigMap) = {
    bucketName = configMap.get("bucketName").fold("")(_.toString)
    fileName = configMap.get("fileName").fold("")(_.toString)
    clientId = configMap.get("client_id").fold("")(_.toString)
    clientSecret = configMap.get("client_secret").fold("")(_.toString)
    loginURL = configMap.get("loginUrl").fold("")(_.toString)
    awsTokenUrl = configMap.get("awsTokenUrl").fold("")(_.toString)
    datasetid1 = configMap.get("datasetid1").fold("")(_.toString)

    println("bucketName=" + bucketName)
    println("fileName=" + fileName)
  }

  test("Verify whether user can able to get Oauth and AWS token") {
    val restClient = new RestClientUtil()
    // This will create a bucket for storage.
    var accessToken = restClient.getAccessToken(clientId, clientSecret, loginURL)
    //assert(accessToken !== "")
    val tokenInfo = restClient.getAwsAccessTokens(accessToken, awsTokenUrl, datasetid1)
    println("acccessKeyId: "+tokenInfo.acccessKeyId)
    println("SecreteAccessKey: "+tokenInfo.secreteAccessKey)
    println("sessionToken: "+tokenInfo.sessionToken)
    assert(tokenInfo.acccessKeyId !== null)
    assert(tokenInfo.secreteAccessKey !== null)
    assert(tokenInfo.sessionToken !== null)
  }

  test("Verify files in S3 bucket") {
    val restClient = new RestClientUtil()
    // This will create a bucket for storage.
    var accessToken = restClient.getAccessToken(clientId, clientSecret, loginURL)
    //assert(accessToken !== "")
    val tokenInfo = restClient.getAwsAccessTokens(accessToken, awsTokenUrl, datasetid1)
    println("acccessKeyId: "+tokenInfo.acccessKeyId)
    println("SecreteAccessKey: "+tokenInfo.secreteAccessKey)
    println("sessionToken: "+tokenInfo.sessionToken)
    assert(tokenInfo.acccessKeyId !== null)
    assert(tokenInfo.secreteAccessKey !== null)
    assert(tokenInfo.sessionToken !== null)

    // Getting list of files in given S3 bucket.
    val filesListInS3bucket = list_objects(bucketName, tokenInfo.acccessKeyId, tokenInfo.secreteAccessKey, tokenInfo.sessionToken);
    assert(filesListInS3bucket !== null)
    assert(filesListInS3bucket.size >0)
  }


  def list_objects(bucketName: String, acccessKeyId: String, secreteAccessKey: String, sessionToken: String): List[String] = {
    val sessionCredentials = new BasicSessionCredentials(acccessKeyId, secreteAccessKey, sessionToken)
    println("sessionCredentials: "+ sessionCredentials.toString)
    val s3_client = AmazonS3ClientBuilder.standard.withCredentials(new AWSStaticCredentialsProvider(sessionCredentials)).build()
    var final_list: List[String] = List()
    var list: List[String] = List()
    val request: ListObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName)
    var result: ListObjectsV2Result = new ListObjectsV2Result()
    do {
      result = s3_client.listObjectsV2(request)
      println("Response from AWS: "+ result)
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


