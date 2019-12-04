package com.lavapm.persona

import com.lavapm.utils.Hdfs
import org.apache.spark.sql.SparkSession



/**
  * Extract the adx interest data from persona.user_info, save in to persona.user_interest_info with pivot model.
  * Created by dalei on 8/21/18.
  */
object ProcessAdxInterest {

  // the column position in hive table person.user_info
  val interestTagPosition = 80
  val imeiEncPosition = 51
  val idfaPosition = 52
  val genderPosition = 4
  val ageRangePosition = 6


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
              .builder()
              .appName("Process the Adx interest")
              .getOrCreate()

    val adxColumn = Array("device_id", "gender", "age_range", "interest_tag", "lava_id")
    val interestColumn = Array("device_id", "gender", "age_range", "digital", "it", "automobile",
                                "finance", "clothing", "jewelry", "cosmetics", "education", "game", "healthy", "furnishing",
                                "childcare", "sport", "culture", "food", "travel", "tobacco_wine", "office_supplies",
                                "real_estate", "industry", "agriculture", "service")

    val interestColumnType = Array("string", "string", "string", "int", "int", "int",
                                    "int", "int", "int", "int", "int", "int", "int", "int",
                                    "int", "int", "int", "int", "int", "int", "int",
                                    "int", "int", "int", "int")

    val hdfsPathSrc = "/user/hive/warehouse/persona.db/user_info"
    val hdfsPathTar = "/user/dmp_app_user/user_interest_info"

    val map_l1 = Map( "消费数码" -> "digital",
                      "IT及信息产业" -> "it",
                      "汽车" -> "automobile",
                      "金融财经" -> "finance",
                      "服装" -> "clothing",
                      "首饰饰品" -> "jewelry",
                      "美容及化妆品" -> "cosmetics",
                      "教育培训" -> "education",
                      "游戏" -> "game",
                      "健康医疗" -> "healthy",
                      "家居生活" -> "furnishing",
                      "母婴育儿" -> "childcare",
                      "运动健身" -> "sport",
                      "文化娱乐" -> "culture",
                      "食品美食" -> "food",
                      "旅游商旅" -> "travel",
                      "烟酒" -> "tobacco_wine",
                      "办公" -> "office_supplies",
                      "房地产" -> "real_estate",
                      "工业" -> "industry",
                      "农业" -> "agriculture",
                      "服务" -> "service")

    val interestArray = Array( "消费数码",
                      "IT及信息产业",
                      "汽车",
                      "金融财经",
                      "服装",
                      "首饰饰品",
                      "美容及化妆品",
                      "教育培训",
                      "游戏",
                      "健康医疗",
                      "家居生活",
                      "母婴育儿",
                      "运动健身",
                      "文化娱乐",
                      "食品美食",
                      "旅游商旅",
                      "烟酒",
                      "办公",
                      "房地产",
                      "工业",
                      "农业",
                      "服务")

    println("**************** before extractAdxInterest2UserInterestInfo ")
    extractAdxInterest2UserInterestInfo(spark, hdfsPathSrc, hdfsPathTar, interestArray)

    println("**************** before createTargetTableInHive ")
    createTargetTableInHive(spark, hdfsPathTar, interestColumn, interestColumnType)

    spark.stop
  }


  /**
    * extract adx interest data in persona.user_info into tmp.user_interest_info
    * @param spark            spark session
    * @param hdfsPathSrc      hdfs path for the persona.user_info
    * @param hdfsPathTar      hdfs path for the tmp.user_interest_info
    * @param interestArray    level 1 interest category name array
    */
  def extractAdxInterest2UserInterestInfo(spark: SparkSession,
                                          hdfsPathSrc: String,
                                          hdfsPathTar: String,
                                          interestArray: Array[String]): Unit = {
    try{
      // read from the hdfs and process each column
      //.sample(true, 0.000001, 12345)
      val rdd = spark.read.parquet(hdfsPathSrc).rdd                  
                  .filter { case row => (row.getAs[String]("interest_tag") != null && row.getAs[String]("interest_tag").trim.length > 0)}
                  .map { case row =>

        val lava_id = row.getString(0).toString
        val strBui = new StringBuilder(lava_id)
        strBui.append("\t")

        // fill the device_id by imei_enc or idfa
        if (row.getAs[String]("imei_enc") != null && row.getAs[String]("imei_enc").trim.length > 0)
          strBui.append(row.getAs[String]("imei_enc") + "\t")
        else if (row.getAs[String]("idfa") != null && row.getAs[String]("idfa").trim.length > 0)
          strBui.append(row.getAs[String]("idfa") + "\t")
        else
          strBui.append("none\t")


        // fill the gender
        if (row.getAs[String]("gender") != null && row.getAs[String]("gender").trim.length > 0)
          strBui.append(row.getAs[String]("gender") + "\t")
        else
          strBui.append("none\t")


        // fill the age_range
        if (row.getAs[String]("age_range") != null && row.getAs[String]("age_range").trim.length > 0)
          strBui.append(row.getAs[String]("age_range") + "\t")
        else
          strBui.append("none\t")


        // check the interest_tag
        if (row.getAs[String]("interest_tag") != null && row.getAs[String]("interest_tag").trim.length > 0) {
          val interest_tag = row.getAs[String]("interest_tag")

          for (cat <- interestArray){
            if (interest_tag.indexOf(cat)> -1)
              strBui.append("1\t")
            else
              strBui.append("0\t")
          }
        }

        strBui.toString
      }

      // delete the target hdfs path, and save the resutl to it
      Hdfs.deleteFile(spark, hdfsPathTar)
      rdd.saveAsTextFile(hdfsPathTar)

    } catch {
      case ex: Exception => {println("*************** error in extractAdxInterest2UserInterestInfo ex := " + ex.toString)}
    }
  }


  /**
    * create the hive table
    * @param spark                spark session
    * @param hdfsPathTar          hdfs path for the result
    * @param interestColumn       column name array for the hive table persona.user_interest_info
    * @param interestColumnType   column type array for the hive table persona.user_interest_info
    */
  def createTargetTableInHive(spark: SparkSession,
                              hdfsPathTar: String,
                              interestColumn: Array[String],
                              interestColumnType: Array[String]): Unit = {

    // prepare the column for hive table
    val columns = new StringBuilder("lava_id string")
    for (i <- 0 to interestColumn.length -1) {
      columns.append(", " + interestColumn(i) + " " + interestColumnType(i))
    }

    // drop the result external table
    spark.sql("drop table if exists tmp.user_interest_info")

    // create the external table
    spark.sql(s"create external table tmp.user_interest_info ( " + columns.toString + ") "
                    + s" row format delimited fields terminated by '\t' lines terminated by '\n' "
                    + s" stored as textfile location '" + hdfsPathTar + "'")

    // drop the target table
    spark.sql("drop table if exists persona.user_interest_info")
    spark.sql(s"create table persona.user_interest_info stored as parquet as select * from tmp.user_interest_info")
  }

}
