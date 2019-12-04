package com.lavapm.persona

import com.lavapm.utils.Hdfs
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession



/**
  * UserInfo
  * Created by dalei on 8/31/18.
  */
object UserInfo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                .builder()
                .appName("Process the UserInfo")
                .getOrCreate()

    val columns = Array(("basic", "name"), ("basic", "name_en"), ("basic", "id_no"), ("basic", "gender"),
                        ("basic", "age"), ("basic", "age_range"), ("basic", "birthday"), ("basic", "nation"),
                        ("basic", "birth_season"), ("basic", "zodiac"), ("basic", "nationality"), ("basic", "life_stage"),
                        ("basic", "profession"), ("basic", "profession_title"), ("basic", "working_status"), ("basic", "working_years"),
                        ("basic", "industry_type"), ("basic", "education_status"), ("basic", "highest_education"), ("basic", "graduate_school"),
                        ("basic", "graduate_year"), ("basic", "major"), ("basic", "foreign_language"), ("basic", "marital_status"),
                        ("basic", "has_child"), ("basic", "child_number"), ("basic", "yearly_income_range"), ("basic", "income_source"),
                        ("basic", "has_estate"), ("basic", "has_car"), ("basic", "consume_level"), ("basic", "consume_frequency"),
                        ("basic", "online_shopping"), ("basic", "offline_shopping"), ("basic", "offline_entertainment"), ("basic", "financial_activity"),  // 36

                        ("device", "mobile_operator"), ("device", "mobile_brand"), ("device", "mobile_language"), ("device", "mobile_OS"), //4
//                        ("device", "mobile_weight"), ("device", "mobile_height"), ("device", "laptop_brand"), ("device", "laptop_language"),
//                        ("device", "laptop_OS"), ("device", "laptop_browser"), ("device", "other_brand"), ("device", "other_language"),
//                        ("device", "other_OS"), ("device", "other_weight"), ("device", "other_height"), // 15

                        ("id", "bank_no"), ("id", "driving_license"), ("id", "passport"),("id", "mobile"),
                        ("id", "mobile_enc"), ("id", "email"), ("id", "email_enc"), ("id", "mac"),
                        ("id", "mac_enc"), ("id", "imei"), ("id", "imei_enc"), ("id", "idfa"),
                        ("id", "idfa_enc"), ("id", "qq"), ("id", "wechat"), ("id", "baidu_id"), // 16
//                        ("id", "taobao_id"), ("id", "weibo_id"), ("id", "linkin_id"), ("id", "google_id"),
//                        ("id", "facebook_id"), ("id", "instagram_id"), ("id", "twitter_id"), ("id", "cookie"),
//                        ("id", "adx_id"), // 25

//                        ("activity", "mobile_duration"), ("activity", "mobile_status"), ("activity", "bank_no_active"), ("activity", "email_active"),
//                        ("activity", "mac_active"), ("activity", "imei_active"), ("activity", "idfa_active"), ("activity", "qq_active"),
//                        ("activity", "wechat_active"), ("activity", "baidu_active"), ("activity", "taobao_active"), ("activity", "weibo_active"),
//                        ("activity", "linkin_active"), ("activity", "facebook_active"), ("activity", "instagram_active"), ("activity", "twitter_active"),
//                        ("activity", "cookie_active"), ("activity", "adx_active"), //18

                        ("geo", "birth_province"), ("geo", "birth_city"), ("geo", "mobile_province"), ("geo", "mobile_city"),
                        ("geo", "id_no_province"), ("geo", "id_no_city"), ("geo", "id_no_county"), ("geo", "resident_province"),
                        ("geo", "resident_city"), ("geo", "resident_address"), ("geo", "city_level"), ("geo", "ip_address"),
                        ("geo", "trip_country"), ("geo", "trip_city"), ("geo", "landmark"), ("geo", "landmark_distance"),
                        ("geo", "landmark_times_30"), ("geo", "landmark_times_180"), ("geo", "landmark_last_time"), ("geo", "landmark_duration"),
                        ("geo", "weekly_gathering_place"), ("geo", "monthly_gathering_place"), ("geo", "latitude_longitude"), //23

                        ("interest", "interest_tag"), ("interest", "brand_tag"), //2

                        ("tob", "source_system"), ("tob", "registration"), ("tob", "call_center_status"), ("tob", "communication_method"),
                        ("tob", "mktg_transaction"), ("tob", "call_center_vendor"), ("tob", "response_channel"), ("tob", "market_intent_tech"),
                        ("tob", "purchase_intent_tech"), ("tob", "actual_purchase_tech"), ("tob", "is_search"), ("tob", "is_download"),
                        ("tob", "is_open"), ("tob", "is_click") //14
                      )

    val columnType = Array("string", "string", "string", "int", "int", "int",
                          "int", "int", "int", "int", "int", "int", "int", "int",
                          "int", "int", "int", "int", "int", "int", "int",
                          "int", "int", "int", "int")

    val hdfsPathTar = "/user/dmp_app_user/user_info"

    println("**************** before transferUserInfoInHbase2Hive ")
    transferUserInfoInHbase2Hive(spark, hdfsPathTar, columns)

    println("**************** before createTargetTableInHive ")
    createTargetTableInHive(spark, hdfsPathTar, columns)

    spark.stop
  }

  /**
    * transfer persona:user_info in hbase to persona.user_info in hive
    * @param spark            spark session
    * @param hdfsPathTar      hdfs path for the result
    * @param columns          column family and column name array for the hive table persona.user_info
    */
  def transferUserInfoInHbase2Hive(spark: SparkSession,
                                   hdfsPathTar: String,
                                   columns: Array[(String, String)]): Unit = {
    try{
      val sc = spark.sparkContext
      val hbaseConf = HBaseConfiguration.create
      hbaseConf.set(TableInputFormat.INPUT_TABLE, "persona:user_info")

      // get the original rdd
      val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf,
                                        classOf[TableInputFormat],
                                        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                        classOf[org.apache.hadoop.hbase.client.Result])

//      val count = hbaseRDD.count()
//      println("*************** count := " + count)

      // build the result rdd
      val rowRDD = hbaseRDD.map { case (_, result) => {

        // get the rowkey
        val key = Bytes.toString(result.getRow)

        val strBui = new StringBuilder(key)

        for ((family, column) <- columns) {

          val value = Bytes.toString(result.getValue(family.getBytes, column.getBytes))

          strBui.append("\t")

          if(value != null && (! value.equals("")) && (! value.equalsIgnoreCase("null")))
            strBui.append(value.trim.replace("\t", " "))
        }

        strBui.toString
      }
      }

      // delete the target hdfs path, and save the result to it
      Hdfs.deleteFile(spark, hdfsPathTar)
      rowRDD.saveAsTextFile(hdfsPathTar)

    } catch {
      case ex: Exception => {println("*************** error in transferUserInfoInHbase2Hive ex := " + ex.toString)}
    }
  }


  /**
    * create the hive table with new columns
    * @param spark            spark session
    * @param hdfsPathTar      hdfs path for the result
    * @param columns          column family and column name array for the hive table persona.user_info
    */
  def createTargetTableInHive(spark: SparkSession,
                              hdfsPathTar: String,
                              columns: Array[(String, String)]): Unit = {

    // prepare the column for hive table
    val strBuilder = new StringBuilder("lava_id string")
    for ((family, column) <- columns) {
      strBuilder.append(", " + column + " string")
    }

    // drop the result external table
    spark.sql("drop table if exists idc_dmp.user_info")

    // create the external table
    spark.sql(s"create external table idc_dmp.user_info ( " + strBuilder.toString + ") "
                     + s" row format delimited fields terminated by '\t' lines terminated by '\n' "
                     + s" stored as textfile location '" + hdfsPathTar + "'")

    // drop the result table
    spark.sql("drop table if exists persona.user_info")

    // create the table
    spark.sql(s"create table persona.user_info stored as parquet as select * from idc_dmp.user_info")
  }
}
