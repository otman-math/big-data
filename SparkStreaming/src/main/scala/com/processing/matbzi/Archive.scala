package com.processing.matbzi

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics, MultilabelMetrics, binary}
import org.apache.log4j.{Level, Logger}

import scala.util.Try




object Learning
{


  def main(args: Array[String]): Unit =
  {

    //kafka data
    val brokers = "node-5:9092"
    val topics_data = "grp-11-matbzi-data-prod"
    //val topics_vigilances = "grp-11-matbzi-vigilances"
    val groupId = "GRP1"

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingProd")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val sc = ssc.sparkContext
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    //sc.setLogLevel("OFF")

    // Create direct kafka stream with brokers and topics
    val topicsSetData = topics_data.split(",").toSet
    //val topicsSetVigilances = topics_vigilances.split(",").toSet

    //kafka config
    val kafkaParams = Map[String, Object] (
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val dataDS = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSetData, kafkaParams))

    /*val vigilanceDS = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSetVigilances, kafkaParams))*/

    //process json data
    @transient lazy val logger = Logger.getLogger(getClass.getName)

    // So we'll demonstrate using SparkSQL in order to query each RDD
    // using SQL queries.

    val spark = SparkSession
      .builder()
      .appName("Spark Processing Hive")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql



    //Data Indices
    dataDS.foreachRDD( (rddRaw, time)  =>
    {



      //examine prediction
      //println("============================> Prédiction")

      //predictedClassificationbatch.collect().foreach( println(_) )



      // SparkSQL can automatically create DataFrames from Scala "case classes".
      // We created the Record case class for this purpose.
      // So we'll convert each RDD of tuple data into an RDD of "Record"
      // objects, which in turn we can convert to a DataFrame using toDF()
      //val requestsDataFrame = rddRaw.map( w => Data(w._1, w._2, w._3 , w._4 , w._5 , w._6) ).toDF()


      val rdd_indices = rddRaw.map( r => r.value() )
      val dataframe_indices = spark.read.json( rdd_indices )

      val data = dataframe_indices.createOrReplaceTempView("indices")

      val dataframe_indices_hive = sql("SELECT   couleur_html , date_indice , dd , ff, hbas , insee , nbas , pmer , pres ,  qualificatif , rr6 , t , td, tend, type_valeur, u, valeur, vv FROM indices")

      dataframe_indices_hive.printSchema()
      dataframe_indices_hive.show()

      //save in hive
      dataframe_indices_hive.write.mode(SaveMode.Append).format("hive").saveAsTable("grp_11_matbzi_data_demo")


    })

    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("/user/p1926796/project/checkpoint/")

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}

