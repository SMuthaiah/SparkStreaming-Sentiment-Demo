import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo {

  def main(args: Array[String]) {
    // Parse input argument for accessing twitter streaming api
    if (args.length < 4) {
      System.err.println("Usage: Demo <consumer key> <consumer secret> " + "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val conf = new SparkConf().setAppName("Demo").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    // creating twitter input stream
    val tweets = TwitterUtils.createStream(ssc, None, filters)
    // print the original tweets
    //tweets.print()
    // Apply the sentiment analysis using detectSentiment function in the SentimentAnalysisUtils
    tweets.foreachRDD { (rdd, time) =>
      rdd.map(t => {
        Map(
          "user" -> t.getUser.getScreenName,
          "location" -> Option(t.getGeoLocation).map(geo => {
            s"${geo.getLatitude},${geo.getLongitude}"
          }),
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          "retweet" -> t.getRetweetCount,
          "language" -> t.getText,
          "sentiment" -> SentimentAnalysisUtils.detectSentiment(t.getText).toString

        )

      })

     // rdd.saveAsTextFile("output/")

    }

    tweets.print()
    // Start streaming
    ssc.start()
    // Wait for Termination
    ssc.awaitTermination()

  }
}