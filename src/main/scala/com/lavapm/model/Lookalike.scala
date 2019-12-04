package com.lavapm.model


import com.lavapm.utils.Hdfs
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import scala.collection.mutable.ListBuffer


/**
  * Lookalike
  * Created by dalei on 11/24/18.
  */
object Lookalike {

  /**
    * postive audience columns: lava_id, device_id, gender, age_range, 1st level interest ...
    * pool audience columns: lava_id, device_id, gender, age_range, 1st level interest ...
    * result audience columns: lava_id
    */

  val originRddCol = Array("lava_id", "device_id", "gender", "age_range", "digital", "it", "automobile",
                          "finance", "clothing", "jewelry", "cosmetics", "education", "game", "healthy", "furnishing",
                          "childcare", "sport", "culture", "food", "travel", "tobacco_wine", "office_supplies",
                          "real_estate", "industry", "agriculture", "service")

  val numericCol = Array("digital", "it", "automobile",
                        "finance", "clothing", "jewelry", "cosmetics", "education", "game", "healthy", "furnishing",
                        "childcare", "sport", "culture", "food", "travel", "tobacco_wine", "office_supplies",
                        "real_estate", "industry", "agriculture", "service")

  val keyCols = Array("lava_id")

  val oneHotCols = Array("gender", "age_range")

  val schema = StructType(originRddCol.map(fieldName => StructField(fieldName, StringType, true)))



  def main(args: Array[String]): Unit = {

    val posPath = args(0)
    val poolPath = args(1)
    val resPath = args(2)
    val scale = args(3).toInt

//    val posPath = "/user/dmp_app_user/model/lookalike/pos"
//    val poolPath = "/user/dmp_app_user/model/pool"
//    val resPath = "/user/dmp_app_user/model/lookalike/res"
//
//    val scale = 1000000

    val spark = SparkSession
                .builder()
                .appName("Lookalike")
                .getOrCreate()

    println("**************** before trainModel ")
    trainModel(spark, posPath, poolPath, resPath, scale)

    spark.stop()
  }


  /**
    * create lookalike model by positive and pool audience corpus
    * @param spark        spark session
    * @param posPath      positive audience path in hdfs
    * @param poolPath     pool audience path in hdfs
    * @param resPath      result audience path in hdfs
    * @param scale        the scale for enlarging
    */
  def trainModel( spark: SparkSession,
                  posPath: String,
                  poolPath: String,
                  resPath: String,
                  scale: Int) : Unit = {

    // transfer the positive audience from file to data frame
    val posRDD = spark.sparkContext.textFile(posPath).map(_.split("\t", -1)).map { p =>
      var valueSeq: Seq[String] = Seq()
      for (i <- 0 to (originRddCol.length - 1)) valueSeq :+= p(i)
      Row.fromSeq(valueSeq)
    }

    val posDF = spark.createDataFrame(posRDD, schema)
    val posNum = posDF.count.toInt
    posDF.createOrReplaceTempView("pos")


    // transfer the pool audience from file to data frame, default remove the positive audience
    val poolRDD = spark.sparkContext.textFile(poolPath).map(_.split("\t", -1)).map { p =>
      var valueSeq: Seq[String] = Seq()
      for (i <- 0 to (originRddCol.length - 1)) valueSeq :+= p(i)
      Row.fromSeq(valueSeq)
    }

    val poolDF = spark.createDataFrame(poolRDD, schema).except(posDF).limit(1000000)
    poolDF.createOrReplaceTempView("pool")


    // generate the negative audience from poolDF and posDF
    val negDF = poolDF.limit(posNum)
    negDF.createOrReplaceTempView("neg")


    // prepare the training data frame with label and feature, prepare the pool data frame with lava_id and feature
    var featureColStr = ""
    for(col <- oneHotCols) featureColStr += " , " + col
    for(col <- numericCol) featureColStr += " , cast(" + col + " as Double) as " + col

    val trainingDF = spark.sql("select cast(1 as Double) as label " + featureColStr + " from pos").union(spark.sql("select cast(0 as Double) as label " + featureColStr + " from neg"))
    val poolDF1 = spark.sql("select lava_id " + featureColStr + " from pool")


    // create the pipeline
    val stagesArray = new ListBuffer[PipelineStage]()

    // create category index by StringIndexer, then create the vector by OneHotEncoder
    for (col <- oneHotCols) {
      val indexer = new StringIndexer().setInputCol(col).setOutputCol(s"${col}_index")
      val encoder = new OneHotEncoderEstimator().setInputCols(Array(indexer.getOutputCol)).setOutputCols(Array(s"${col}_classVec"))
      //val encoder = new OneHotEncoder().setInputCol(indexer.getOutputCol).setOutputCol(s"${col}classVec").setDropLast(false)
      stagesArray.append(indexer,encoder)
    }


    // prepare the features
    val assemblerInputs = oneHotCols.map(_ + "_classVec") ++ numericCol

    // convert all features to one vector by VectorAssembler
    val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features")
    stagesArray.append(assembler)

    val pipeline = new Pipeline()
    pipeline.setStages(stagesArray.toArray)


    // process the one-hot encoding for training and pool data frame
    val pipelineModel = pipeline.fit(poolDF1)
    val trainingVecDF = pipelineModel.transform(trainingDF)
    val poolVecDF = pipelineModel.transform(poolDF1)

    println(s"trainingVecDF size=${trainingVecDF.count}, poolVecDF size=${poolVecDF.count}")


    // build the model and make the prediction on the pool
    val lrModel = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").fit(trainingVecDF)
    val predictDF = lrModel.transform(poolVecDF).select("lava_id", "prediction")


    // select the alike audience and write to result folder
    Hdfs.deleteFile(spark, resPath)
    println(s"******************* after hdfs.delete()")
    predictDF.orderBy(predictDF("prediction").desc).limit(scale).rdd.map(x => x.get(0)).saveAsTextFile(resPath)

  }

}
