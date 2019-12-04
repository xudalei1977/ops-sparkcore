package com.lavapm.persona

import org.apache.spark.sql.SparkSession

/**
  * transfer prod_dsp1 mysql database adt_prod data to hive database dim_dsp
  * create by ryan on 5/6/2019
  */
object TransferDimDsp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Transfer Dim Dsp Data")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    loadMySQLTable2Hive(spark,"account_advertiser_aptitude")
    println("***********account_advertiser_aptitude table saved************")

    loadMySQLTable2Hive(spark,"account_advertiser_audit")
    println("***********account_advertiser_audit table saved************")

    loadMySQLTable2Hive(spark,"account_advertiser_detail")
    println("***********account_advertiser_detail table saved************")

    loadMySQLTable2Hive(spark,"account_advertiser_share")
    println("***********account_advertiser_share table saved************")

    loadMySQLTable2Hive(spark,"account_agent_apikey")
    println("***********account_agent_apikey table saved************")

    loadMySQLTable2Hive(spark,"account_agent_detail")
    println("***********account_agent_detail table saved************")

    loadMySQLTable2Hive(spark,"account_agent_logo")
    println("***********account_agent_logo table saved************")

    loadMySQLTable2Hive(spark,"account_aptitude_contrast")
    println("***********account_aptitude_contrast table saved************")

    loadMySQLTable2Hive(spark,"account_charge")
    println("***********account_charge table saved************")

    loadMySQLTable2Hive(spark,"account_product")
    println("***********account_product table saved************")

    loadMySQLTable2Hive(spark,"account_relationship")
    println("***********account_relationship table saved************")

    loadMySQLTable2Hive(spark,"account_status")
    println("***********account_status table saved************")

    loadMySQLTable2Hive(spark,"ad_campaign")
    println("***********ad_campaign table saved************")

    loadMySQLTable2Hive(spark,"ad_direct_dsp")
    println("***********ad_direct_dsp table saved************")

    loadMySQLTable2Hive(spark,"ad_group")
    println("***********ad_group table saved************")

    loadMySQLTable2Hive(spark,"ad_group_status")
    println("***********ad_group_status table saved************")

    loadMySQLTable2Hive(spark,"bes_main_aptitude")
    println("***********bes_main_aptitude table saved************")

    loadMySQLTable2Hive(spark,"bidswitch_request")
    println("***********bidswitch_request table saved************")

    loadMySQLTable2Hive(spark,"bill_advertiser_day")
    println("***********bill_advertiser_day table saved************")

    loadMySQLTable2Hive(spark,"bill_adx_day")
    println("***********bill_adx_day table saved************")

    loadMySQLTable2Hive(spark,"bill_agent_charge")
    println("***********bill_agent_charge table saved************")

    loadMySQLTable2Hive(spark,"bill_agent_charge_secret")
    println("***********bill_agent_charge_secret table saved************")

    loadMySQLTable2Hive(spark,"bill_agent_charge_task")
    println("***********bill_agent_charge_task table saved************")

    loadMySQLTable2Hive(spark,"bill_agent_day")
    println("***********bill_agent_day table saved************")

    loadMySQLTable2Hive(spark,"bill_agent_month")
    println("***********bill_agent_month table saved************")

    loadMySQLTable2Hive(spark,"bill_agent_total")
    println("***********bill_agent_total table saved************")

    loadMySQLTable2Hive(spark,"config_area")
    println("***********config_area table saved************")

    loadMySQLTable2Hive(spark,"config_area_map")
    println("***********config_area_map table saved************")

    loadMySQLTable2Hive(spark,"config_category")
    println("***********config_category table saved************")

    loadMySQLTable2Hive(spark,"config_category_map")
    println("***********config_category_map table saved************")

    loadMySQLTable2Hive(spark,"config_chargetype")
    println("***********config_chargetype table saved************")

    loadMySQLTable2Hive(spark,"config_creative_plat")
    println("***********config_creative_plat table saved************")

    loadMySQLTable2Hive(spark,"config_creative_shop")
    println("***********config_creative_shop table saved************")

    loadMySQLTable2Hive(spark,"config_geoip")
    println("***********config_geoip table saved************")

    loadMySQLTable2Hive(spark,"config_media_category")
    println("***********config_media_category table saved************")

    loadMySQLTable2Hive(spark,"config_media_domain")
    println("***********config_media_domain table saved************")

    loadMySQLTable2Hive(spark,"config_media_domain_new")
    println("***********config_media_domain_new table saved************")

    loadMySQLTable2Hive(spark,"config_mobile_network")
    println("***********config_mobile_network table saved************")

    loadMySQLTable2Hive(spark,"config_mobile_operator")
    println("***********config_mobile_operator table saved************")

    loadMySQLTable2Hive(spark,"config_mobile_system")
    println("***********config_mobile_system table saved************")

    loadMySQLTable2Hive(spark,"config_pc_browser")
    println("***********config_pc_browser table saved************")

    loadMySQLTable2Hive(spark,"config_pc_system")
    println("***********config_pc_system table saved************")

    loadMySQLTable2Hive(spark,"config_plantfrom")
    println("***********config_plantfrom table saved************")

    loadMySQLTable2Hive(spark,"config_platid_map")
    println("***********config_platid_map table saved************")

    loadMySQLTable2Hive(spark,"config_region")
    println("***********config_region table saved************")

    loadMySQLTable2Hive(spark,"config_ssp_size")
    println("***********config_ssp_size table saved************")

    loadMySQLTable2Hive(spark,"config_video_category")
    println("***********config_video_category table saved************")

    loadMySQLTable2Hive(spark,"config_video_category_map")
    println("***********config_video_category_map table saved************")

    loadMySQLTable2Hive(spark,"creative")
    println("***********creative table saved************")

    loadMySQLTable2Hive(spark,"creative_audit")
    println("***********creative_audit table saved************")

    loadMySQLTable2Hive(spark,"creative_group")
    println("***********creative_group table saved************")

    loadMySQLTable2Hive(spark,"creative_native_asset")
    println("***********creative_native_asset table saved************")

    loadMySQLTable2Hive(spark,"creative_native_title")
    println("***********creative_native_title table saved************")

    loadMySQLTable2Hive(spark,"creative_size")
    println("***********creative_size table saved************")

    loadMySQLTable2Hive(spark,"creative_template")
    println("***********creative_template table saved************")

    loadMySQLTable2Hive(spark,"creative_template_fields")
    println("***********creative_template_fields table saved************")

    loadMySQLTable2Hive(spark,"dim_app_package")
    println("***********dim_app_package table saved************")

    loadMySQLTable2Hive(spark,"dim_app_package_bes")
    println("***********dim_app_package_bes table saved************")

    loadMySQLTable2Hive(spark,"dim_app_package_bes_table")
    println("***********dim_app_package_bes_table table saved************")

    loadMySQLTable2Hive(spark,"dmp_task")
    println("***********dmp_task table saved************")

    loadMySQLTable2Hive(spark,"domain_blacklist")
    println("***********domain_blacklist table saved************")

    loadMySQLTable2Hive(spark,"finance_advertiser")
    println("***********finance_advertiser table saved************")

    loadMySQLTable2Hive(spark,"finance_trans_logs")
    println("***********finance_trans_logs table saved************")

    loadMySQLTable2Hive(spark,"goods_recommend")
    println("***********goods_recommend table saved************")

    loadMySQLTable2Hive(spark,"hibernate_sequence")
    println("***********hibernate_sequence table saved************")

    loadMySQLTable2Hive(spark,"ip_blacklist")
    println("***********ip_blacklist table saved************")

    loadMySQLTable2Hive(spark,"ip_rt")
    println("***********ip_rt table saved************")

    loadMySQLTable2Hive(spark,"ip_whitelist")
    println("***********ip_whitelist table saved************")

    loadMySQLTable2Hive(spark,"log_adgroup")
    println("***********log_adgroup table saved************")

    loadMySQLTable2Hive(spark,"log_dberror")
    println("***********log_dberror table saved************")

    loadMySQLTable2Hive(spark,"log_operation")
    println("***********log_operation table saved************")

    loadMySQLTable2Hive(spark,"mail_list")
    println("***********mail_list table saved************")

    loadMySQLTable2Hive(spark,"message_detail")
    println("***********message_detail table saved************")

    loadMySQLTable2Hive(spark,"message_receive")
    println("***********message_receive table saved************")

    loadMySQLTable2Hive(spark,"native_template_asset")
    println("***********native_template_asset table saved************")

    loadMySQLTable2Hive(spark,"private_db")
    println("***********private_db table saved************")

    loadMySQLTable2Hive(spark,"privilege")
    println("***********privilege table saved************")

    loadMySQLTable2Hive(spark,"product")
    println("***********product table saved************")

    loadMySQLTable2Hive(spark,"role")
    println("***********role table saved************")

    loadMySQLTable2Hive(spark,"template_type")
    println("***********template_type table saved************")

    loadMySQLTable2Hive(spark,"template_type_fields")
    println("***********template_type_fields table saved************")

    loadMySQLTable2Hive(spark,"tool_contract_split")
    println("***********tool_contract_split table saved************")

    loadMySQLTable2Hive(spark,"tool_transform")
    println("***********tool_transform table saved************")

    spark.stop()
  }


  /**
    * load mysql table in adt_prod to hive dim_dsp database
    * @param    spark       spark session
    * @param    tableName   table name
    */
  def loadMySQLTable2Hive(spark: SparkSession,tableName:String) = {
    val dataFrame = spark
      .read
      .format("jdbc")
      .options(Map(
        "url" -> "jdbc:mysql://rm-uf6c35061h14f949k.mysql.rds.aliyuncs.com:3306/adt_prod",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> tableName,
        "user" -> "lava_dsp",
        "password" -> "LavaPM2018"))
      .load()

    spark.sql("drop table if exists dim_dsp."+ tableName +"")
    dataFrame.write.format("hive").saveAsTable("dim_dsp."+ tableName +"")

  }
}
