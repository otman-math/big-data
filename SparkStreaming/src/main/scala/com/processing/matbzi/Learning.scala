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

    val dataDF = sql("select * from grp_11_matbzi_data")

    println("Préparation de données : ")

    dataDF.printSchema()
    dataDF.show()

     val NaiveBayesDataSet = dataDF
       .filter(line => line.get(1) != null &&
         line.get(3) != null  &&
         line.get(6) != null  &&
         line.get(7) != null  &&
         line.get(13) != null  &&
         line.get(14) != null  &&
         line.get(1).toString.toDouble >= 0 &&
         line.get(3).toString.toDouble >= 0 &&
         line.get(6).toString.toDouble >= 0 &&
         line.get(7).toString.toDouble >= 0 &&
         line.get(13).toString.toDouble >= 0 &&
         line.get(14).toString.toDouble >= 0
       )
       .map( line =>
         LabeledPoint(line.get(16).toString.toDouble, Vectors.dense(line.get(1).toString.toDouble, line.get(3).toString.toDouble, line.get(6).toString.toDouble,line.get(7).toString.toDouble, line.get(13).toString.toDouble, line.get(14).toString.toDouble  )))


    println()
    println()
    println("Training & testing : ")
    NaiveBayesDataSet.collect().take(10).foreach( println(_))
    println()
    println()

     val allData = NaiveBayesDataSet.randomSplit( Array(.85 , .15) , 13L)

     val trainingDataSet = allData(0)
     val testDataSet = allData(1)

     println("number of training data =",trainingDataSet.count())

     println("number of test data =",testDataSet.count())


     // build model
     val myNaiveBayesModel = NaiveBayes.train( trainingDataSet.rdd , lambda = 1.0, modelType = "multinomial")

     //classify
     val predictedClassification = testDataSet.map( x => (myNaiveBayesModel.predict(x.features), x.label))

     //examine prediction
     predictedClassification.collect().foreach(println(_))

     //Metrics
     val metrics = new MulticlassMetrics(predictedClassification.rdd)
     val confusionMatrix = metrics.confusionMatrix
     println("Confusion Matrix= n",confusionMatrix)

     val myModelStat=Seq(metrics.precision,metrics.recall)

     println("Précision - Recall : ")
     myModelStat.foreach( println(_) )




    //Data Indices
    dataDS.foreachRDD( (rddRaw, time)  =>
    {

      val rdd_indices = rddRaw.map( r => r.value() )

      val dataframe_indices = spark.read.json( rdd_indices )

      dataframe_indices.printSchema()
      dataframe_indices.show()

      val NaiveBayesDataBatchSet = dataframe_indices
        .filter(line => line.get(12) != null &&
          line.get(14) != null  &&
          line.get(4) != null  &&
          line.get(5) != null  &&
          line.get(18) != null  &&
          line.get(19) != null  &&
          line.get(12).toString.toDouble >= 0 &&
          line.get(14).toString.toDouble >= 0 &&
          line.get(4).toString.toDouble >= 0 &&
          line.get(5).toString.toDouble >= 0 &&
          line.get(18).toString.toDouble >= 0 &&
          line.get(19).toString.toDouble >= 0
        )
        .map( line =>
          LabeledPoint(line.get(11).toString.toDouble, Vectors.dense(line.get(12).toString.toDouble, line.get(14).toString.toDouble, line.get(4).toString.toDouble,line.get(5).toString.toDouble, line.get(18).toString.toDouble, line.get(19).toString.toDouble  )))

      println()
      println()
      println("============> Batch : ")
      NaiveBayesDataBatchSet.collect().take(10).foreach( println(_))
      println()
      println()

      //classify
      val predictedClassificationbatch = NaiveBayesDataBatchSet.map( x => (myNaiveBayesModel.predict(x.features), x.label))

      predictedClassificationbatch.collect().foreach(
        x => {
          if(x._1 == 1.0)
          {

            println("C'est Bon ! ");
            rdd_indices.saveAsTextFile ("/user/p1926796/project/bon.txt");

          }
          else
          {

            println("Oppps, L'alerte est enregistré dans /user/p1926796/project/alerts.txt ");
            rdd_indices.saveAsTextFile ("/user/p1926796/project/alerts.txt");

          }


        }
      )

      //examine prediction
      //println("============================> Prédiction")

      //predictedClassificationbatch.collect().foreach( println(_) )



      // SparkSQL can automatically create DataFrames from Scala "case classes".
      // We created the Record case class for this purpose.
      // So we'll convert each RDD of tuple data into an RDD of "Record"
      // objects, which in turn we can convert to a DataFrame using toDF()
      //val requestsDataFrame = rddRaw.map( w => Data(w._1, w._2, w._3 , w._4 , w._5 , w._6) ).toDF()


      /*val rdd_indices = rddRaw.map( r => r.value() )
      val dataframe_indices = spark.read.json( rdd_indices )

      val data = dataframe_indices.createOrReplaceTempView("indices")

      val dataframe_indices_hive = sql("SELECT   couleur_html , date_indice , dd , ff, hbas , insee , nbas , pmer , pres ,  qualificatif , rr6 , t , td, tend, type_valeur, u, valeur, vv FROM indices")

      dataframe_indices_hive.printSchema()
      dataframe_indices_hive.show()

      //save in hive
      dataframe_indices_hive.write.mode(SaveMode.Append).format("hive").saveAsTable("grp_11_matbzi_data_demo")*/


    })

    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("/user/p1926796/project/checkpoint/")

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}

