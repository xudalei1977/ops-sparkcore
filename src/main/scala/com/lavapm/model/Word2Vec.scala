package com.lavapm.model

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{Word2Vec, _}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions._


/**
  * Map the app name to interest by word2vec model.
  * Created by dalei on 11/3/18.
  */
object Word2Vec {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                .builder()
                .appName("Word2Vec")
                .config("spark.defalut.parallelism", "10")
                .getOrCreate()


    val corpusPath = "/user/dmp_app_user/corpus"
    val stopWordPath = "/user/dmp_app_user/stop_word"
    val modelPath = "/user/dmp_app_user/model/word2vec"
    val interestPath = "/user/dmp_app_user/original_data/dim_dsp/dim_interest"
    val appPath = "/user/hive/warehouse/dim_dsp.db/dim_app_package"
    val appInterestPath = "/user/dmp_app_user/dim_app_interest"

    println("**************** before createWord2VecModel() ")
    createWord2VecModel(spark, corpusPath, stopWordPath, modelPath)

    println("**************** before mapApp2Interest() ")
    mapApp2Interest(spark, stopWordPath, modelPath, interestPath, appPath, appInterestPath)

    spark.stop()
  }



  /**
    * create Word2Vec model by specific corpus
    * @param spark          spark session
    * @param corpusPath     corpus path in hdfs
    * @param stopWordPath   stop word path in hdfs
    * @param modelPath      model saved path in hdfs
    */
  def createWord2VecModel( spark: SparkSession,
                           corpusPath: String,
                           stopWordPath: String,
                           modelPath: String) : Unit = {

    val stopWords = spark.sparkContext.broadcast(spark.read.textFile(stopWordPath).collect().toSet)

    import spark.implicits._

    val df = spark.read.textFile(corpusPath).map{ line =>
      val words = new JiebaSegmenter().sentenceProcess(line)
      words.toArray.map(_.toString).map(_.trim).filter(_.length > 0).filter(!stopWords.value.contains(_))
    }.map(Tuple1.apply).toDF("text")

    // create the word2vec model
    val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0)
    val model = word2Vec.fit(df)

    model.save(modelPath)
  }



  /**
    * map the app to interest by the word2vec model
    * @param spark            spark session
    * @param stopWordPath     stop word path in hdfs
    * @param modelPath        model saved path in hdfs
    * @param interestPath     interst path in hdfs
    * @param appPath          app path in hdfs
    * @param appInterestPath  app interest mapping relation path in hdfs
    */
  def mapApp2Interest( spark: SparkSession,
                       stopWordPath: String,
                       modelPath: String,
                       interestPath: String,
                       appPath: String,
                       appInterestPath: String) : Unit = {

    val stopWords = spark.sparkContext.broadcast(spark.read.textFile(stopWordPath).collect().toSet)

    // load the word2vec model
    val model = Word2VecModel.load(modelPath)

    import spark.implicits._

    // read the interest
    val interestDF = spark.read.textFile(interestPath).filter(_.indexOf("-")>0).map{line =>
      val parts = line.split(",")
      val interest = parts(0)

      val words = new JiebaSegmenter().sentenceProcess(interest)
      (words.toArray.map(_.toString).map(_.trim).filter(_.length > 0).filter(!stopWords.value.contains(_)), interest)
    }.toDF("text", "interest")

    // get the interest vector and read them to array
    val interestVecArray = model.transform(interestDF.select("text"))
                              .join(interestDF, Seq("text"), joinType = "inner")
                              .select("interest", "result")
                              .rdd.map{row => (row.getString(0), row.get(1).asInstanceOf[Vector])}
                              .collect

    // read the app name
//    val appDF = spark.read.parquet(appPath).filter(!_.toString.toLowerCase.contains("null")).map{line =>
//      val parts = line.split("\001")
//      val packageName = parts(0)
//      val appName = parts(1)
//
//      val words = new JiebaSegmenter().sentenceProcess(appName)
//      (words.toArray.map(_.toString).map(_.trim).filter(_.length > 0).filter(!stopWords.value.contains(_)), packageName, appName)
//    }.toDF("text", "package", "appName").distinct

    // read the app name
    val appDF = spark.read.parquet(appPath).filter("package_name is not null").filter("app_name is not null").rdd.map { case row =>

      val words = new JiebaSegmenter().sentenceProcess(row.getAs[String]("app_name"))
      (words.toArray.map(_.toString).map(_.trim).filter(_.length > 0).filter(!stopWords.value.contains(_)), row.getAs[String]("package_name"), row.getAs[String]("app_name"))

    }.toDF("text", "package", "appName").distinct


    // get the app vector
    val appVec = model.transform(appDF.select("text"))
                      .join(appDF, Seq("text"), joinType = "inner")
                      .select("package", "appName", "result").distinct

    appVec.rdd.map{row =>
      val appVec = row.get(2).asInstanceOf[Vector]
      var nearestInterest = ""
      var nearestDistance = 10000.0

      for((interest, interestVec) <- interestVecArray){
        val dis = euclideanDis(appVec, interestVec)
        if(dis < nearestDistance){
          nearestInterest = interest
          nearestDistance = dis
        }
      }

      row.getString(0) + "\t" + row.get(1) + "\t" + nearestInterest
    }.saveAsTextFile(appInterestPath)

//    // calculate all app and interest distance
//    val disVec = appVec.select("appName", "result")
//                       .join(interestVec)
//                       .select(appVec("appName"), interestVec("interest"), euclideanDistance(appVec("result"), interestVec("result")).alias("distance"))
//
//    // get the minimum distance and mapped the interest
//    val mappedVec = disVec.groupBy("appName")
//                          .min("distance")
//                          .join(disVec, Seq("distance"), joinType = "inner")
//                          .select(disVec("appName"), disVec("interest"))
//
//    // mapped the package name and write to hdfs
//    app.join(mappedVec, app("appName") === mappedVec("appName"), joinType = "inner")
//        .select(app("package"), app("appName"), mappedVec("interest"))
//        .rdd.map(x => x.get(0) + "\t" + x.get(1) + "\t" + x.get(2)).saveAsTextFile(appInterestPath)

  }


  /**
    * transfer column type from WrappedArray to String
    */
  def castArray2String = udf((column1: collection.mutable.WrappedArray[String]) => {
    column1.mkString("")
  })


  /**
    * define a udf to calculate the euclidean distance of two vector column
    */
  def euclideanDistance = udf((column1: Vector, column2: Vector) => {
    math.sqrt(column1.toArray.zip(column2.toArray).map(p => p._1 - p._2).map(d => d*d).sum)
  })


  /**
    * define a function to calculate the euclidean distance of two vector column
    */
  def euclideanDis (column1: Vector, column2: Vector) : Double = {
    math.sqrt(column1.toArray.zip(column2.toArray).map(p => p._1 - p._2).map(d => d*d).sum)
  }

}

