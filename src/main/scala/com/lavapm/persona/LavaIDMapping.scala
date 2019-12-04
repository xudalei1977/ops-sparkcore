package com.lavapm.persona

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Mapping lava_id from user id like imei,idfa,mobile,etc.
  * Create by ryan on 06/06/2019.
  */
object LavaIDMapping {
  def main(args: Array[String]): Unit = {
    val batch_no = args(0)
    val hdfsPath = s"/user/dmp_app_user/upload/"+ batch_no +""

    val spark = SparkSession
      .builder()
      .appName("Mapping Lava ID")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    lavaIDMapping(spark,batch_no,hdfsPath)

    spark.stop()
  }


  def lavaIDMapping (spark:SparkSession,batch_no:String,hdfsPath:String) = {
    val dataFrame = spark
      .read
      .format("json")
      .load(hdfsPath)

    val columnsArr = dataFrame.columns

    dataFrame.createOrReplaceGlobalTempView("id_table")

    val sql = new StringBuilder("select distinct b.lava_id from global_temp.id_table a,persona.user_info b where ")

    for (column <- columnsArr) sql.append("a."+column+" = b."+column+" or ")

    println("***sql***:"+sql.substring(0,sql.length-3))

    spark
      .sql(sql.substring(0, sql.length - 3))
      .createOrReplaceGlobalTempView("lava_id_batchNo")

    //only overwrite the specific partition
    spark.sql("insert overwrite table persona.user_info_ready partition(batch_no='"+batch_no+"') select * from global_temp.lava_id_batchNo")

  }


}
