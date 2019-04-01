import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object CapstoneConsumer {
  def main (args: Array[String]): Unit = {

    //set the topics, Spark configuration, Kafka params, and schema
    val topics = List("twitter-test-scala").toSet
    val conf = new SparkConf().setAppName("Capstone Consumer").setMaster("local[*]")
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "capstone"
    )
    val schema = StructType(List(
      StructField("created_at", StringType, true),
      StructField("text", StringType, true),
      StructField("user", StructType(List(
        StructField("screen_name", StringType, true),
        StructField("followers_count", LongType, true),
        StructField("friends_count", LongType, true),
        StructField("location", StringType, true)
      )), true)
    ))


    //Set up streaming context and set log level to only show error messages
    val ssc = new StreamingContext(conf, Seconds(60))
    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")

    //get a Dstream from the Kafka broker
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)).map(record => record.value)


    //run a loop on each RDD in the Dstream
    stream.foreachRDD { (rdd, time ) =>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()

      //Create a DataFrame from the schema and the RDD
      val twitterDF = spark.sqlContext.read.schema(schema).json(rdd)

      // Create a sql-readable view from the DataFrame
      twitterDF.createOrReplaceTempView("tweets")

      // Process the DataFrame into the correct number of columns
      val finalDF = spark.sql("select created_at, text, user.screen_name, user.followers_count, user.friends_count, user.location from tweets")

      //Show the final DataFrame
      println(s"========= $time =========")
      finalDF.show()

      //Write the DataFrame to file
      val ws = finalDF.write.mode("append").json("/home/evan/TwitterConsumer")
//      val ws = finalDF.coalesce(1).rdd.saveAsTextFile("hdfs:///user/maria_dev/TwitterConsumer/")
  }

    stream.print()
//    ssc.start()
    ssc.awaitTermination()


  }
}
