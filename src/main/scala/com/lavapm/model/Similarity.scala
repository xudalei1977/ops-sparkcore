package com.lavapm.model

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors, DenseVector => SDV}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector => BDV}
import com.lavapm.utils.Hdfs
import org.apache.spark.ml.stat.Summarizer

import scala.math._


/**
  * Similarity Algorithm by vector distance
  * Created by dalei on 5/20/19.
  */
object Similarity {

  /**
    * postive audience columns: lava_id, device_id, gender, age_range, 1st level interest ...
    * pool audience columns: lava_id, device_id, gender, age_range, 1st level interest ...
    * result audience columns: lava_id
    */

  val zero = 1.0E-6
  val infinite = 1.0E+6

  val originRddCol = Array("lava_id", "device_id", "gender", "age_range", "digital", "it", "automobile",
                           "finance", "clothing", "jewelry", "cosmetics", "education", "game", "healthy", "furnishing",
                           "childcare", "sport", "culture", "food", "travel", "tobacco_wine", "office_supplies",
                           "real_estate", "industry", "agriculture", "service")

  def main(args: Array[String]): Unit = {

    val posPath = args(0)
    val poolPath = args(1)
    val resPath = args(2)
    val scale = args(3).toInt

//    val posPath = "/user/dmp_app_user/model/similarity/pos/123456"
//    val poolPath = "/user/dmp_app_user/model/pool"
//    val resPath = "/user/dmp_app_user/model/similarity/res"
//
//    val scale = 1000000

    val spark = SparkSession
                .builder()
                .appName("Similarity")
                .getOrCreate()

    println("**************** before trainModel ")
    trainModel(spark, posPath, poolPath, resPath, scale)

    spark.stop()
  }


  /**
    * create similarity model by specific corpus
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

    import spark.implicits._
    import Summarizer._

    // read the positive audience from file to RDD[Array[String]]
    val posRDD = spark.sparkContext.textFile(posPath).map(_.split("\t", -1))

    // transform the RDD to a Matrix the mean center of the positive audience
    val posVecDF = posRDD.map { p =>
      var valueSeq: Seq[Double] = Seq()

      valueSeq :+= hashGender(p(2))
      valueSeq :+= hashAgeRange(p(3))

      for (i <- 4 to (originRddCol.length - 1)) valueSeq :+= p(i).toDouble

      (Vectors.dense(valueSeq.toArray), 1.0)
    }.toDF("features", "weight")

    //val centerVec = Statistics.colStats(posVecRDD).mean.toDense
    val meanVec = posVecDF.select(mean($"features")).first().getAs[Vector](0).toDense

    // read the pool audience from file to RDD[Array[String]]
    val poolRDD = spark.sparkContext.textFile(poolPath).map(_.split("\t", -1)).map { p =>
      var valueSeq: Seq[Double] = Seq()

      valueSeq :+= hashGender(p(2))
      valueSeq :+= hashAgeRange(p(3))

      for (i <- 4 to (originRddCol.length - 1)) valueSeq :+= p(i).toDouble

      val vec = Vectors.dense(valueSeq.toArray).toDense

      val distance = ItemDistance(meanVec, vec)

      //Row.fromSeq(Seq(p(0), distance))
      (p(0), distance)
    }

    // val schema = StructType(List(StructField("lava_id", StringType, true), StructField("distance", DoubleType, true)))
    // val poolDF = spark.createDataFrame(poolRDD, schema)

    val res = poolRDD.sortBy(_._2).take(scale)

    Hdfs.deleteFile(spark, resPath)
    spark.sparkContext.parallelize(res).map(_._1).saveAsTextFile(resPath)
    // poolDF.orderBy(poolDF("distance")).limit(scale).rdd.map(x => x.get(0)).saveAsTextFile(resPath)
  }


  /**
    * one-hot the gender
    * @param  gender       the gender as string
    * @return Double       the gender as double
    */
  def hashGender(gender: String): Double = {
    gender match {
      case "M" => 1
      case "F" => 2
      case _ => 0
    }
  }


  /**
    * one-hot the gender
    * @param  ageRange     the age range as string
    * @return Double       the age range as double
    */
  def hashAgeRange(ageRange: String): Double = {
    ageRange match {
      case "18-" => 1
      case "19~24" => 2
      case "25~29" => 3
      case "30~34" => 4
      case "35~39" => 5
      case "40~49" => 6
      case "50+" => 7
      case _ => 0
    }
  }


  /**
    * Calculate the distance between two vectors.
    * @param vec1_svd         spark DenseVector
    * @param vec2_svd         spark DenseVector
    * @return Double          the distance between two vectors
    */
  def ItemDistance(vec1_svd: SDV,
                   vec2_svd: SDV): Double = {

    val vec1_norm = Vectors.norm(vec1_svd, 2)
    val vec2_norm = Vectors.norm(vec2_svd, 2)

    if(vec1_norm < zero)
      return infinite

    if(vec2_norm < zero)
      return infinite

    val vec1_bdv = new BDV(vec1_svd.toDense.values)
    val vec2_bdv = new BDV(vec2_svd.toDense.values)

    val vec = Vectors.dense(2.0, 3.0, 5.0)
    vec.toArray
    //calculate the distance between vector1 and vector2
    Vectors.norm(new SDV((vec1_bdv - vec2_bdv).data), 2) / (vec1_norm * vec2_norm)
  }


  /**
    * Calculate the distance from an item to a group.
    * @param item_rdd               item group
    * @param outside_item           outside item
    * @return Double                summary of the distance from an item to a group
    */
  def ItemDistance(item_rdd: RDD[Array[Double]],
                   outside_item: Array[Double]): Double = {

    val vec1_bdv = new BDV(outside_item)
    val vec1_norm = Vectors.norm(new SDV(outside_item), 2)

    if(vec1_norm < zero)
      return infinite

    //calculate the distance from outside_item to item_group
    val dist_rdd = item_rdd.filter(f => f.map(abs(_)).sum > zero).map(f => {

//      if (f.length != outside_item.length)
//        throw Exception

      val vec2_bdv = new BDV(f)

      Vectors.norm(new SDV((vec1_bdv - vec2_bdv).data), 2) / (vec1_norm * Vectors.norm(new SDV(f), 2))
    })

    dist_rdd.sum
  }


  /**
    * Calculate the distance from an item to a group.
    * @param item_rdd               item group
    * @param outside_item           outside item
    * @return Double                summary of the distance from an item to a group
    */
  def ItemDistance(item_rdd: RDD[SDV],
                   outside_item: SDV): Double = {


    val vec1_bdv = new BDV(outside_item.toDense.values)
    val vec1_norm = Vectors.norm(outside_item, 2)

    if(vec1_norm < zero)
      return infinite

    //calculate the distance from outside_item to item_group
    val dist_rdd = item_rdd.filter(f => Vectors.norm(f, 2) > zero).map(f => {

//    if (f.size != outside_item.size)
//      throw Exception
      val vec2_bdv = new BDV(f.toDense.values)

      Vectors.norm(new SDV((vec1_bdv - vec2_bdv).data), 2) / (vec1_norm * Vectors.norm(f, 2))
    })

    dist_rdd.sum
  }

}
