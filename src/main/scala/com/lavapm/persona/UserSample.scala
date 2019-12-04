package com.lavapm.persona

import org.apache.spark.sql.SparkSession

/**
  * take sample from persona.user_info.
  * create by Ryan on 12/08/2019.
  */
object UserSample {
  def main(args: Array[String]): Unit = {
    val batch_no = args(0)
    val number = args(1).toDouble
    val fraction = number/945905236

    val spark = SparkSession
      .builder()
      .appName("Take sample for user ")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    takeSample(spark,fraction,batch_no)
    println("************table saved")

    spark.stop()
  }



  def takeSample(spark:SparkSession,fraction:Double,batch_no:String) = {

    spark
      .sql("select lava_id from persona.user_info")
      .sample(fraction)
      .createOrReplaceGlobalTempView("lava_id_sample")

    //only overwrite the specific partition
    spark.sql("insert overwrite table persona.user_info_ready partition(batch_no='"+batch_no+"') select * from global_temp.lava_id_sample")

  }
}
