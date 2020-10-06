package com.aws.scala.s3client

import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap, FunSuite}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3, AmazonS3Client}
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, S3ObjectSummary}
import com.amazonaws.ClientConfiguration

import scala.collection.JavaConverters.asScalaBufferConverter

class AwsS3Test extends FunSuite with BeforeAndAfterAllConfigMap {
  var clientId = ""
  var clientSecret = ""
  var grantType = ""
  var loginURL = ""
  var awsTokenUrl = ""
  var bucketName = ""
  var fileName = ""
  var datasetid1 = ""
  var datasetid2 = ""
  var datasetid3 = ""
  var datasetid4 = ""
  var datasetid5 = ""
  var datasetid6 = ""
  var datasetid7 = ""
  var proxyUrl = ""
  var proxyPort = ""
  var profileName=""

  override def beforeAll(configMap: ConfigMap) = {
    bucketName = configMap.get("bucketName").fold("")(_.toString)
    fileName = configMap.get("fileName").fold("")(_.toString)
    clientId = configMap.get("client_id").fold("")(_.toString)
    clientSecret = configMap.get("client_secret").fold("")(_.toString)
    grantType = configMap.get("grant_type").fold("")(_.toString)
    loginURL = configMap.get("loginUrl").fold("")(_.toString)
    awsTokenUrl = configMap.get("awsTokenUrl").fold("")(_.toString)
    datasetid1 = configMap.get("datasetid1").fold("")(_.toString)
    datasetid2 = configMap.get("datasetid2").fold("")(_.toString)
    datasetid3 = configMap.get("datasetid3").fold("")(_.toString)
    datasetid4 = configMap.get("datasetid4").fold("")(_.toString)
    datasetid5 = configMap.get("datasetid5").fold("")(_.toString)
    datasetid6 = configMap.get("datasetid6").fold("")(_.toString)
    datasetid7 = configMap.get("datasetid7").fold("")(_.toString)

    proxyUrl = configMap.get("proxyUrl").fold("")(_.toString)
    proxyPort = configMap.get("proxyPort").fold("")(_.toString)

    profileName = configMap.get("profileName").fold("")(_.toString)

    println("bucketName=" + bucketName)
    println("fileName=" + fileName)

  }

  test("Verify files count in S3 bucket for dataset1 (session token)") {
    val restClient = new RestClientUtil()

    // Get OAuthTokens.
    val accessToken = restClient.getAccessToken(clientId, clientSecret, grantType, loginURL)
    assert(accessToken !== "")

    // Get AWS tokens.
    val tokenInfo = restClient.getAwsAccessTokens(accessToken, awsTokenUrl, datasetid1)
    assert(tokenInfo.acccessKeyId !== null)
    assert(tokenInfo.secreteAccessKey !== null)
    assert(tokenInfo.sessionToken !== null)

    // Initialize AWS client
    val awsClientUtil = new AwsClientUtil(proxyUrl, proxyPort, bucketName, fileName,
      tokenInfo.acccessKeyId, tokenInfo.secreteAccessKey);

    // Getting list of files in given S3 bucket.
    val filesListInS3bucket = awsClientUtil.listObjectsWithSessionCredentials(tokenInfo.sessionToken)
    assert(filesListInS3bucket !== null)
    assert(filesListInS3bucket.size > 0)
  }

  test("Verify files count in S3 bucket for dataset1 (profile)") {
    val restClient = new RestClientUtil()

    // Get OAuthTokens.
    val accessToken = restClient.getAccessToken(clientId, clientSecret, grantType, loginURL)
    assert(accessToken !== "")

    // Get AWS tokens.
    val tokenInfo = restClient.getAwsAccessTokens(accessToken, awsTokenUrl, datasetid1)
    assert(tokenInfo.acccessKeyId !== null)
    assert(tokenInfo.secreteAccessKey !== null)
    assert(tokenInfo.sessionToken !== null)

    // Initialize AWS client
    val awsClientUtil = new AwsClientUtil(proxyUrl, proxyPort, bucketName, fileName,
      tokenInfo.acccessKeyId, tokenInfo.secreteAccessKey);

    // Getting list of files in given S3 bucket.
    val filesListInS3bucket = awsClientUtil.listObjectsWithProfile(profileName)
    assert(filesListInS3bucket !== null)
    assert(filesListInS3bucket.size > 0)
  }

}


