package com.lavapm.dmp

import com.lavapm.utils.Hdfs
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession
import com.lavapm.utils.Analysis


/**
  * StatisticsTag is calling a script to calculate a crowd.
  * Created by dalei on 5/28/19.
  */
object StatisticsTag {


  def main(args: Array[String]): Unit = {

    val tableName = args(0)
    val keyColumn = args(1)
    val valueColumn = args(2)
    val range = args(3)
    val resPath = args(4)

//    val tableName = "sandbox_cisco.sql_booking_rawdata"
//    val keyColumn = "mobile_enc"
//    val valueColumn = "count"
//    val range = "1st-quarter"
//    val resPath = "/user/dmp_app_user/model/systemTag/res"


    val spark = SparkSession
      .builder()
      .appName("Statistics Tag")
      .getOrCreate()

    println("**************** before generateAudienceByTag ")
    generateAudienceByTag(spark, tableName, keyColumn, valueColumn, range, resPath)

    spark.stop()
  }


  /**
    * generate audience by system tag.
    *
    * @param spark          spark session
    * @param tableName      table name, this table could be persona.user_info or customer sandbox
    * @param keyColumn      key column, e.g. "mobile", which is used to map to "lava_id",
    *                       if the IDKey is "lava_id" already, the mapping is unnecessary
    * @param valueColumn    value column, could be some keyword, e.g. "count"
    * @param range          the audience range, e.g. "1st quarter"
    * @param resPath        result audience path in hdfs
    */
  def generateAudienceByTag(spark: SparkSession,
                            tableName: String,
                            keyColumn: String,
                            valueColumn: String,
                            range: String,
                            resPath: String) : Unit = {

    import spark.implicits._

    // prepare the sql sentence to read the column to be analysed
    var sql = s"select " + keyColumn + " , "

    val columnClause = valueColumn match {
      case "count" => "cast(count(1) as double) as count"
      case _ => "cast(" + valueColumn + " as double) as " + valueColumn
    }

    sql += columnClause + " from " + tableName + " "

    val groupClause = valueColumn match {
      case "count" => " group by " + keyColumn
      case _ => " "
    }

    sql += groupClause

    println("**************** sql := " + sql)

    //val sql = "select busphone_enc, cast(count(1) as double) as count from sandbox_cisco.sql_booking_rawdata group by busphone_enc"

    val rdd = spark.sql(sql).select(valueColumn).rdd.map{ row =>
      row.getDouble(0)
    }

    // get the statistic information for the column value
    val (mean, variance, count, max, min, normL1, normL2, quartile1, median, quartile3) = Analysis.vectorStatistic(spark, rdd)

    val expr = range match {
      case "1st-quarter" => valueColumn + " <= " + quartile1
      case "2nd-quarter" => valueColumn + " > " + quartile1 + " and " + valueColumn + " <= " + median
      case "3rd-quarter" => valueColumn + " > " + median + " and " + valueColumn + " <= " + quartile3
      case "4th-quarter" => valueColumn + " > " + quartile3
      case "1st-half" => valueColumn + " <= " + median
      case "2nd-half" => valueColumn + " > " + median
    }

    val df1 = spark.sql(sql).filter(expr).select(keyColumn).distinct()

    // get the key mapping table to look for lava_id
    val sql2 = keyColumn match {
      case "mobile" => "select mobile, lava_id from persona.user_mobile"
      case "mobile_enc" => "select mobile_enc, lava_id from persona.user_mobile_enc"
    }

    val df2 = spark.sql(sql2)

    // find the lava_id of the target audience, and write the lava_id to hdfs
    Hdfs.deleteFile(spark, resPath)
    df1.join(df2, keyColumn).select(df2("lava_id")).rdd.map(x => x.get(0)).saveAsTextFile(resPath)

  }
}
