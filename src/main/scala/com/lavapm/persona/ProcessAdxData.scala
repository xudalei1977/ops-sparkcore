package com.lavapm.persona

import com.lavapm.utils._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession


/**
  * ProcessAdxData
  * Created by dalei on 6/20/2018.
  */

object ProcessAdxData {

  def main(args: Array[String]): Unit = {

    //val (map_l1, map_l2) = Hive.generateInterestCategoryMap

    val spark = SparkSession
                .builder()
                .appName("Process the Adx data")
                .getOrCreate()


    val adxColumn = Array("device_id", "gender", "age_range", "interest_tag", "lava_id")
    val interestColumn = Array("imei_enc", "gender", "age_range", "interest_tag")

    val hdfsPathSrc = "/user/hive/warehouse/tmp.db/adx_user"

    println("**************** before createSourceTableInHive() ")
    createSourceTableInHive(spark)

    println("**************** before extractRadData2UserInterestInfo() ")
    extractAdxData2UserInterestInfo(spark, hdfsPathSrc)

    spark.stop
  }


  /**
    * create the hive table to add lava_id column to adx table
    * @param spark           spark session
    */
  def createSourceTableInHive(spark: SparkSession): Unit = {

    // drop the result table firstly
    spark.sql("drop table if exists tmp.adx_user")

    // create the table
    spark.sql(s"create table tmp.adx_user stored AS parquet as " +
                      s" select a.*, b.lava_id from adx_data.adx_user a " +
                      s" left outer join tmp.user_basic_info b on a.device_id = b.imei_enc where length(a.device_id) = 32 union" +
                      s" select a.*, b.lava_id from adx_data.adx_user a " +
                      s" left outer join tmp.user_basic_info b on a.device_id = b.idfa where length(a.device_id) > 32 ")

  }


  /**
    * extract adx data in hive into persona:user_interest_info in hbase
    *
    * @param spark       spark session
    * @param hdfsPathSrc hdfs path for the adx interest data
    */
  def extractAdxData2UserInterestInfo(spark: SparkSession,
                                      hdfsPathSrc: String): Unit = {
    try {

      // read from the hdfs and process each column
      val rdd = spark.read.parquet(hdfsPathSrc).rdd.map { case row =>

        /**
          * check whether the imei_enc exists in persona:user_id_info.
          * if yes, this is the same user or device, return the existing lava_id
          * if not, this is a new user or device, create a new lava_id
          */
        // check the lava_id
        var lava_id = ""
        if (row.getAs[String]("lava_id") != null && (!row.getAs[String]("lava_id").equals("")) && (!row.getAs[String]("lava_id").equalsIgnoreCase("NULL")))
          lava_id = row.getAs[String]("lava_id").trim
        else
          lava_id = UUID.generateUUID

        val put = new Put(Bytes.toBytes(lava_id))

        val device_id = row.getAs[String]("device_id")

        if (device_id.indexOf("-") > 0) { // the device id is an idfa
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("idfa"), Bytes.toBytes(device_id))
          put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes("iPhone"))
          put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_os"), Bytes.toBytes("iOS"))
        } else { // the device id is an imei
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("imei_enc"), Bytes.toBytes(device_id))
        }

        // check the gender
        if (row.getAs[String]("gender") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("gender"), Bytes.toBytes(row.getAs[String]("gender").trim))

        // check the age_range
        if (row.getAs[String]("age_range") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("age_range"), Bytes.toBytes(row.getAs[String]("age_range").trim))

        // check the interest_tag
        if (row.getAs[String]("interest_tag") != null)
          put.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(row.getAs[String]("interest_tag").trim))


        // check the interest_tag
        //      if (writables(3) != null) {
        //        val category_id_l1 = new StringBuilder
        //        val category_name_l1 = new StringBuilder
        //        val category_id_l2 = new StringBuilder
        //        val category_name_l2 = new StringBuilder
        //
        //        val tag_array = writables(3).toString.split(",")
        //        for(tag <- tag_array){
        //          if (tag.indexOf("-") > 0) {
        //            //if(map_l2.contains(tag)) category_id_l2.append("," + map_l2(tag))
        //            category_name_l2.append("," + tag)
        //          } else {
        //            if(map_l1.contains(tag)) category_id_l1.append("," + map_l1(tag))
        //            category_name_l1.append("," + tag)
        //          }
        //        }
        //
        //        if (category_id_l1.length > 0)
        //          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("category_id_l1"), Bytes.toBytes(category_id_l1.deleteCharAt(0).toString))
        //
        //        if (category_name_l1.length > 0)
        //          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("category_name_l1"), Bytes.toBytes(category_name_l1.deleteCharAt(0).toString))
        //
        ////        if (category_id_l2.length > 0)
        ////          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("category_id_l2"), Bytes.toBytes(category_id_l2.deleteCharAt(0).toString))
        //
        //        if (category_name_l2.length > 0)
        //          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("category_name_l2"), Bytes.toBytes(category_name_l2.deleteCharAt(0).toString))
        //      }


        put
      }

      HBase.bulkLoad(spark,rdd)

    } catch {
      case ex: Exception => {
        println("*************** ex := " + ex.toString)
      }
    }
  }
}
