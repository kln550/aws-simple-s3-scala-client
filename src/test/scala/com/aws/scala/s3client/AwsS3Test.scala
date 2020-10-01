package com.aws.scala.s3client

import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap, FunSuite}

class AwsS3Test extends FunSuite with BeforeAndAfterAllConfigMap {
  var clientId = ""
  var clientSecret = ""
  var loginURL = ""
  var awsTokenUrl = ""
  var bucketName = ""
  var fileName = ""
  //val provider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY))
  //val amazonS3Client = AmazonS3ClientBuilder.standard().withCredentials(provider).withRegion("us-east-1").build()

  override def beforeAll(configMap: ConfigMap) = {
    bucketName = configMap.get("bucketName").fold("")(_.toString)
    fileName = configMap.get("fileName").fold("")(_.toString)
    clientId = configMap.get("client_id").fold("")(_.toString)
    clientSecret = configMap.get("client_secret").fold("")(_.toString)
    loginURL = configMap.get("loginUrl").fold("")(_.toString)
    awsTokenUrl = configMap.get("awsTokenUrl").fold("")(_.toString)

    println("bucketName=" + bucketName)
    println("fileName=" + fileName)
  }

  test("Verify whether user can able to get Oauth and AWS token") {
    val restClient = new RestClientUtil()
    // This will create a bucket for storage.
    var accessToken = restClient.getAccessToken(clientId, clientSecret, loginURL)
    assert(accessToken !== "")
    val tokenInfo = restClient.getAwsAccessTokens(accessToken, awsTokenUrl)
    println("acccessKeyId: "+tokenInfo.acccessKeyId)
    println("SecreteAccessKey: "+tokenInfo.secreteAccessKey)
    println("sessionToken: "+tokenInfo.sessionToken)
    assert(tokenInfo.acccessKeyId !== null)
    assert(tokenInfo.secreteAccessKey !== null)
    assert(tokenInfo.sessionToken !== null)
  }

}
