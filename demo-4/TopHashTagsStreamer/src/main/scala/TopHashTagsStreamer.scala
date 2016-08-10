package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming._
import twitter4j.Status

object TopHashTagsStreamer{
  def main(args: Array[String]) = {
    System.setProperty("twitter4j.oauth.consumerKey", "qdeSRAQuphKwjr1PkhgrZI6ai")
    System.setProperty("twitter4j.oauth.consumerSecret", "jHjdZ2VBS5Oerfm8DcLy0ZelJBk0jcY68G63B40GZDRaBmh6Pe")
    System.setProperty("twitter4j.oauth.accessToken", "58560500-llUH4QnxjuYZc2J6OvgbF2lTuRrdsFv1n4n3NmJAg")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "9YFet3NxkRj1S4MVQq3p9wGyleWIrRT2PUrgHWNB8pPyH")
	
    val conf = new SparkConf().setAppName("Top Hash Tags")
    val sc = new SparkContext(conf)
	val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("file:///StreamingCheckpoint")

	val twitterStream = TwitterUtils.createStream(ssc, None)
    twitterStream.flatMap(tweet=>getHashTags(tweet))
      .countByValueAndWindow(Seconds(15), Seconds(10))
      .map(tagCountTuple => tagCountTuple.swap)
      .transform(rdd => rdd.sortByKey(false))
      .print
	
	ssc.start
	ssc.awaitTermination
  }
  
  def getHashTags(tweet: Status) = {
    raw"(?:(?<=\s)|^)#(\w*[A-Za-z_]+\w*)".r 
	  .findAllIn(tweet.getText).matchData
	  .map(x=>x.group(1)).toList
  }

}