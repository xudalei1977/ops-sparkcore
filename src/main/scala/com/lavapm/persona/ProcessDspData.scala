package com.lavapm.persona

import java.util.regex.Pattern

import com.lavapm.utils.{Hdfs, Hive, Json, UUID}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Process Data from DSP
  * Create by ryan on 02/01/19
  */
object ProcessDspData {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Process the dsp data")
      .enableHiveSupport()
      //set hive table dynamic import
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()

    //activity path for saving the activity data
    val activityPath_dsp_imp = "/user/dmp_app_user/tmp/dsp_imp"
    val activityPath_dsp_clk = "/user/dmp_app_user/tmp/dsp_clk"

    println("**************** before transfer dsp ask data to HBase")
    transferDspAskData2HBase(spark)

    println("**************** before transfer dsp imp data to activity data")
    transferDspImpData2ActivityData(spark,activityPath_dsp_imp)

    println("**************** before transfer dsp clk data to activity data")
    transferDspClkData2ActivityData(spark,activityPath_dsp_clk)

    println("**************** before transfer dsp activity data to hive")
    transferActivityData2Hive(spark)

    spark.stop()
  }


  /**
    * transfer dsp ask data to hbase table persona:user_info
    * @param spark   spark session
    */
  def transferDspAskData2HBase(spark: SparkSession): Unit={

    //get the map of district to "province,city"
    val locationMap = Hive.generateCity2Province(spark)
    //get the map of city to city level
    val cityLevelMap = Hive.generateCityName2CityLevel(spark)
    //get the map of package name to interest
    val packageInterestMap = Hive.generateApp2InterestMap(spark)

    val sc = spark.sparkContext
    sc.broadcast(locationMap)
    sc.broadcast(cityLevelMap)
    sc.broadcast(packageInterestMap)

    try{
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "persona:user_info")

      lazy val job = Job.getInstance(hbaseConf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

      //get several data frame from fact_dsp.fact_dsp_ask according to device id types
      val (imeiDF,imeiEncDF,idfaDF,idfaEncDF) = getDataFrameFromAskData(spark)

      //there are several data type
      val dfType_imei = "imei"
      val dfType_imei_enc = "imei_enc"
      val dfType_idfa = "idfa"
      val dfType_idfa_enc = "idfa_enc"

      // process the data
      val imeiHBaseRDD = transferDspAskData2HBaseRDD(spark,imeiDF,dfType_imei,locationMap,cityLevelMap,packageInterestMap)
      val imeiEncHBaseRDD = transferDspAskData2HBaseRDD(spark,imeiEncDF,dfType_imei_enc,locationMap,cityLevelMap,packageInterestMap)
      val idfaHBaseRDD = transferDspAskData2HBaseRDD(spark,idfaDF,dfType_idfa,locationMap,cityLevelMap,packageInterestMap)
      val idfaEncHBaseRDD = transferDspAskData2HBaseRDD(spark,idfaEncDF,dfType_idfa_enc,locationMap,cityLevelMap,packageInterestMap)

      // union all of the HBase RDDs
      val hbaseRDD = imeiHBaseRDD.union(imeiEncHBaseRDD).union(idfaHBaseRDD).union(idfaEncHBaseRDD)

      //println(hbaseRDD.count())
      hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration())
      println("*********** HBase table saved")
    } catch {
      case ex: Exception => {println("*************** ex := " + ex.toString)}
    }
  }


  /**
    * transfer dsp imp data to activity data and save in hdfs
    * @param spark   spark session
    * @param activityPath   hdfs path for the activity data
    */
  def transferDspImpData2ActivityData(spark: SparkSession,activityPath:String): Unit={

    // create data frame of different type of device id and get device id to row pair rdd
    val (activityImeiRDD,activityImeiEncRDD,activityIdfaRDD,activityIdfaEncRDD) = getImpDataDeviceId2RowPairRDD(spark)

    // get device id to lava id pairRDD from local data base persona user info
    val (userImeiRDD,userImeiEncRDD,userIdfaRDD,userIdfaEncRDD) = getUserInfoDeviceId2LavaIdPairRDD(spark)

    val activityType_imp = "imp"
    // process the data
    val imeiHiveRDD = transferActivityData2HiveRDD(activityImeiRDD,userImeiRDD,activityType_imp)
    val imeiEncHiveRDD = transferActivityData2HiveRDD(activityImeiEncRDD,userImeiEncRDD,activityType_imp)
    val idfaHiveRDD = transferActivityData2HiveRDD(activityIdfaRDD,userIdfaRDD,activityType_imp)
    val idfaEncHiveRDD = transferActivityData2HiveRDD(activityIdfaEncRDD,userIdfaEncRDD,activityType_imp)

    val hiveRDD = imeiHiveRDD.union(imeiEncHiveRDD).union(idfaHiveRDD).union(idfaEncHiveRDD)
    // delete the target hdfs path if exists
    Hdfs.deleteFile(spark, activityPath)
    hiveRDD.saveAsTextFile(activityPath)
    println("******************************* after imp data saving as text file")
  }


  /**
    * transfer dsp clk data to activity data and save in hdfs
    * @param spark   spark session
    * @param activityPath   hdfs path for the activity data
    */
  def transferDspClkData2ActivityData(spark: SparkSession,activityPath:String): Unit={

    // create data frame of different type of device id and get device id to row pair rdd
    val (activityImeiRDD,activityImeiEncRDD,activityIdfaRDD,activityIdfaEncRDD) = getClkDataDeviceId2RowPairRDD(spark)

    // get device id to lava id pairRDD from local data base persona user info
    val (userImeiRDD,userImeiEncRDD,userIdfaRDD,userIdfaEncRDD) = getUserInfoDeviceId2LavaIdPairRDD(spark)

    val activityType_clk = "clk"
    // process the data
    val imeiHiveRDD = transferActivityData2HiveRDD(activityImeiRDD,userImeiRDD,activityType_clk)
    val imeiEncHiveRDD = transferActivityData2HiveRDD(activityImeiEncRDD,userImeiEncRDD,activityType_clk)
    val idfaHiveRDD = transferActivityData2HiveRDD(activityIdfaRDD,userIdfaRDD,activityType_clk)
    val idfaEncHiveRDD = transferActivityData2HiveRDD(activityIdfaEncRDD,userIdfaEncRDD,activityType_clk)

    val hiveRDD = imeiHiveRDD.union(imeiEncHiveRDD).union(idfaHiveRDD).union(idfaEncHiveRDD)
    // delete the target hdfs path if exists
    Hdfs.deleteFile(spark, activityPath)
    hiveRDD.saveAsTextFile(activityPath)
    println("******************************* after clk data saving as text file")
  }


  /**
    * get several data frame from dsp ask data according to different device id type
    * @param spark   spark session
    */
  def getDataFrameFromAskData(spark: SparkSession) = {
    val imeiDF = spark
      .sql("SELECT b.lava_id,a.* FROM (SELECT mobile_brand,language,mobile_imei,mobile_mac,mobile_geo_district,ip,mobile_app_bundle" +
        " FROM fact_dsp.fact_dsp_ask WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-08-14' AND length(mobile_imei) > 0" +
        " AND length(mobile_imei) != 32) a LEFT OUTER JOIN persona.user_imei b ON a.mobile_imei = b.imei WHERE b.imei IS NOT NULL")

    val imeiEncDF = spark
      .sql("SELECT b.lava_id,a.* FROM (SELECT mobile_brand,language,mobile_imei,mobile_mac,mobile_geo_district,ip,mobile_app_bundle" +
        " FROM fact_dsp.fact_dsp_ask WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-08-14' AND length(mobile_imei) = 32) a" +
        " LEFT OUTER JOIN persona.user_imei_enc b ON a.mobile_imei = b.imei_enc WHERE b.imei_enc IS NOT NULL")

    val idfaDF = spark
      .sql("SELECT b.lava_id,a.* FROM (SELECT mobile_brand,language,mobile_idfa,mobile_mac,mobile_geo_district,ip,mobile_app_bundle" +
        " FROM fact_dsp.fact_dsp_ask WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-08-14' AND length(mobile_idfa) > 0" +
        " AND length(mobile_idfa) != 32) a LEFT OUTER JOIN persona.user_idfa b ON a.mobile_idfa = b.idfa WHERE b.idfa IS NOT NULL")

    val idfaEncDF = spark
      .sql("SELECT b.lava_id,a.* FROM (SELECT mobile_brand,language,mobile_idfa,mobile_mac,mobile_geo_district,ip,mobile_app_bundle" +
        " FROM fact_dsp.fact_dsp_ask WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-08-14' AND length(mobile_idfa) = 32) a" +
        " LEFT OUTER JOIN persona.user_idfa_enc b ON a.mobile_idfa = b.idfa_enc WHERE b.idfa_enc IS NOT NULL")

    (imeiDF,imeiEncDF,idfaDF,idfaEncDF)
  }


  /**
    *  transfer different device type of dsp ask data to HBase RDD, this is for saving in HBase later
    * @param spark                spark session
    * @param askDataDF            ask data frame
    * @param dfType               data frame type
    * @param locationMap          map from district to "province,city" string
    * @param cityLevelMap         map from city to city level
    * @param packageInterestMap   map from package name to interest
    */
  def transferDspAskData2HBaseRDD(spark: SparkSession,
                                  askDataDF:Dataset[Row],
                                  dfType:String,
                                  locationMap:Map[String,String],
                                  cityLevelMap:Map[String,String],
                                  packageInterestMap:Map[String,String]) = {

    val noLavaIdDF = askDataDF.filter("lava_id IS NULL")
    val lavaIdDF = askDataDF.filter("lava_id IS NOT NULL")

    println("***********************************lava id is null and create new one************************************")
    val askDataNoLavaId2InterestRDD = getAskDataNoLavaId2InterestRDD(spark,noLavaIdDF,packageInterestMap,dfType)

    val deviceId2RowRDD = noLavaIdDF.rdd
      .map(row => {
        (getDeviceId(row,dfType),row)
      })


    val noLavaIdHBaseRDD = deviceId2RowRDD
      .join(askDataNoLavaId2InterestRDD)
      .map(line => {
        val lava_id = line._2._2.split("-")(0)
        val put = new Put(lava_id.getBytes())
        val row = line._2._1
        var interest = ""
        if (line._2._2.split("-").length == 2)
          interest = line._2._2.split("-")(1)

        addColunmToHBasePut(put,row,locationMap,cityLevelMap,dfType,interest)
      })


    println("**********************************lava id is all ready in our database***********************************")
    val askDataLavaId2InterestRDD = getAskDataLavaId2InterestRDD(spark,lavaIdDF,packageInterestMap,dfType)
    val userInfoLavaId2InterestRDD = Hive.transferInterestFromUserInfo2UserInterestRDD(spark)

    val lavaId2CombinedInterestRDD = askDataLavaId2InterestRDD
      .join(userInfoLavaId2InterestRDD)
      .map(line => {
        if (line._2._1 != ""){
          if (line._2._2 == null || line._2._2 == "")
            (line._1, line._2._1)
          else
            (line._1, line._2._1 + "," + line._2._2)
        } else {
          if (line._2._2 == null || line._2._2 == "")
            (line._1, "")
          else
            (line._1, line._2._2)
        }
      })

    val lavaId2RowRDD = lavaIdDF.rdd.map(row => (row.getAs[String]("lava_id"),row))

    val lavaIdHBaseRDD = lavaId2RowRDD
      .join(lavaId2CombinedInterestRDD)
      .map(line => {
        val lava_id = line._1
        val put = new Put(lava_id.getBytes())
        val row = line._2._1
        val interest = line._2._2
        addColunmToHBasePut(put, row, locationMap, cityLevelMap, dfType, interest)
      })

    println("****************************before union HBase rdd")
    lavaIdHBaseRDD.union(noLavaIdHBaseRDD)

  }


  /**
    * create lava id for device id and get device id to "lava_id-interest" RDD from ask data frame where lava id is null
    * @param spark                spark session
    * @param noLavaIdDF           ask data frame where lava id is null
    * @param packageInterestMap   map from package name to interest
    * @param dfType               data frame type
    */
  def getAskDataNoLavaId2InterestRDD(spark:SparkSession,
                                     noLavaIdDF:Dataset[Row],
                                     packageInterestMap:Map[String,String],
                                     dfType:String):RDD[(String,String)] = {

    noLavaIdDF.rdd
      .map(row => {
        val device_id = getDeviceId(row,dfType)
        var interest, packageName = ""
        if (row.getAs[String]("mobile_app_bundle") != null)
          packageName = row.getAs[String]("mobile_app_bundle").trim

        if (packageInterestMap.contains(packageName)) {
          interest = packageInterestMap(packageName)
          if (interest.indexOf("-") > 0)
            interest = interest.split("-")(0) + "," + interest
        }

        (device_id, interest)
      })
      //reduce interests of same device id
      .reduceByKey(_ + "," + _)
      //transfer multiple "," to single "," and create lava_id for each device id
      .map(line => {
      val regEx = "[',']+"
      val pattern = Pattern.compile(regEx)
      val matcher = pattern.matcher(line._2)
      var value = matcher.replaceAll(",")
      if (value.length > 0 && value.charAt(0) == ',')
        value = value.replaceFirst(",", "")
      //create lava id for the distinct device id
      (line._1, UUID.generateUUID() + "-" + value)
    })
  }


  /**
    * get lava id to interest RDD from ask data where device id is already in our database
    * @param spark                spark session
    * @param lavaIdDF             data frame that lava id is not null from specific device id type
    * @param packageInterestMap   map from package name to interest
    * @param dfType               data frame type according to device id type
    */
  def getAskDataLavaId2InterestRDD(spark:SparkSession,
                                   lavaIdDF:DataFrame,
                                   packageInterestMap:Map[String,String],
                                   dfType:String)= {

    lavaIdDF.rdd
      .map(row => {
        val lava_id = row.getAs[String]("lava_id").trim
        var interest, packageName = ""

        if (row.getAs[String]("mobile_app_bundle") != null)
          packageName = row.getAs[String]("mobile_app_bundle").trim

        if (packageInterestMap.contains(packageName)) {
          interest = packageInterestMap(packageName)
          if (interest.indexOf("-") > 0)
            interest = interest.split("-")(0) + "," + interest
        }

        (lava_id, interest)
      })
      //reduce interests of same device id
      .reduceByKey(_ + "," + _)
      //transfer multiple "," to single "," and create lava_id for each device id
      .map(line => {
      val regEx = "[',']+"
      val pattern = Pattern.compile(regEx)
      val matcher = pattern.matcher(line._2)
      var value = matcher.replaceAll(",")
      if (value.length > 0 && value.charAt(0) == ',')
        value = value.replaceFirst(",", "")

      (line._1, value)
    })
  }


  /**
    * transfer dsp ask data to HBase put for saving in HBase
    * @param put            HBase put
    * @param row            data frame row
    * @param locationMap    map from district name to "province,city" name string
    * @param cityLevelMap   map from city to city level
    * @param dfType         data frame type
    * @param interest       interest from ask data or the combination of ask data and persona user info
    */
  def addColunmToHBasePut(put: Put,
                          row: Row,
                          locationMap:Map[String,String],
                          cityLevelMap:Map[String,String],
                          dfType:String,
                          interest:String) ={
    //check the mobile_brand
    val mobile_brand = row.getAs[String]("mobile_brand")
    if (mobile_brand != null && mobile_brand.trim != "")
      put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes(mobile_brand.trim))

    //check the language
    val language = row.getAs[String]("language")
    if (language != null && language.trim != "")
      put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_language"), Bytes.toBytes(language.trim))

    //check the location include province,city and city level
    val city = row.getAs[String]("mobile_geo_city").trim
    if (city != null && city != "") {
      put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_city"), Bytes.toBytes(city))

      if (locationMap.contains(city))
        put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_province"), Bytes.toBytes(locationMap(city)))

      if (cityLevelMap.contains(city))
        put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("city_level"), Bytes.toBytes(cityLevelMap(city)))

    }

    val district = row.getAs[String]("mobile_geo_district").trim
    if (district != null && district != "")
      put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_district"), Bytes.toBytes(district))

    //check the ip
    val ip = row.getAs[String]("ip")
    if (ip != null && ip.trim != "")
      put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("ip_address"), Bytes.toBytes(ip.trim))

    //check the interest
    if (interest != "")
      put.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(interest))

    //check the device id
    put.addColumn(Bytes.toBytes("id"), Bytes.toBytes(dfType), Bytes.toBytes(getDeviceId(row,dfType)))

    //check the mac
    val mobile_mac = row.getAs[String]("mobile_mac")
    if (mobile_mac != null && mobile_mac.trim != "") { //the device id = "" or null,then check mac
      if (mobile_mac.trim.length == 32) //this is a mac_enc
        put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac_enc"), Bytes.toBytes(mobile_mac.trim))
      else //this is a mac
        put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac"), Bytes.toBytes(mobile_mac.trim))
    }

    (new ImmutableBytesWritable(), put)
  }


  /**
    * get device id from different data frame type
    * @param row      data frame row
    * @param dfType   data frame type include imei, imei_enc, idfa, idfa_enc
    */
  def getDeviceId(row: Row,dfType: String):String = {
    if (dfType == "imei" || dfType == "imei_enc")
      row.getAs[String]("mobile_imei").trim
    else
      row.getAs[String]("mobile_idfa").trim
  }


  /**
    * get device id to lava id pair rdd from local data base persona user info
    * @param spark   spark session
    */
  def getUserInfoDeviceId2LavaIdPairRDD(spark: SparkSession)= {

    val userImeiRDD = spark
      .sql("SELECT lava_id,imei FROM persona.user_imei WHERE imei IS NOT NULL")
      .rdd.map(row => (row.getAs[String]("imei"),row.getAs[String]("lava_id")))


    val userImeiEncRDD = spark
      .sql("SELECT lava_id,imei_enc FROM persona.user_imei_enc WHERE imei_enc IS NOT NULL")
      .rdd.map(row => (row.getAs[String]("imei_enc"),row.getAs[String]("lava_id")))



    val userIdfaRDD = spark
      .sql("SELECT lava_id,idfa FROM persona.user_idfa WHERE idfa IS NOT NULL")
      .rdd.map(row => (row.getAs[String]("idfa"),row.getAs[String]("lava_id")))



    val userIdfaEncRDD = spark
      .sql("SELECT lava_id,idfa_enc FROM persona.user_idfa_enc WHERE idfa_enc IS NOT NULL")
      .rdd.map(row => (row.getAs[String]("idfa_enc"),row.getAs[String]("lava_id")))



    (userImeiRDD,userImeiEncRDD,userIdfaRDD,userIdfaEncRDD)
  }


  /**
    * create data frame of different type of device id and get device id to row pair rdd from dsp imp data
    * @param spark   spark session
    */
  def getImpDataDeviceId2RowPairRDD(spark: SparkSession)= {

    val activityImeiRDD = spark
      .sql("SELECT a.*,b.country,b.province,b.city,b.district FROM (SELECT mobile_imei,et_imp,ip,mobile_app_bundle," +
        "concat_ws('-',dt_y,dt_m,dt_d) AS dt FROM fact_dsp.fact_dsp_imp WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-09-01' " +
        "AND length(mobile_imei) > 0 AND length(mobile_imei) != 32 AND concat_ws('-',id,et) " +
        "NOT IN (SELECT concat_ws('-',id,et) FROM fact_dsp.fact_dsp_clk)) AS a LEFT OUTER JOIN tmp.dsp_ip_location b ON a.ip = b.ip")
      .rdd.map(row => (row.getAs[String]("mobile_imei"),row))


    val activityImeiEncRDD = spark
      .sql("SELECT a.*,b.country,b.province,b.city,b.district FROM (SELECT mobile_imei,et_imp,ip,mobile_app_bundle," +
        "concat_ws('-',dt_y,dt_m,dt_d) AS dt FROM fact_dsp.fact_dsp_imp WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-09-01' " +
        "AND length(mobile_imei) = 32 AND concat_ws('-',id,et) NOT IN (SELECT concat_ws('-',id,et) FROM fact_dsp.fact_dsp_clk)) " +
        "AS a LEFT OUTER JOIN tmp.dsp_ip_location b ON a.ip = b.ip")
      .rdd.map(row => (row.getAs[String]("mobile_imei"),row))


    val activityIdfaRDD = spark
      .sql("SELECT a.*,b.country,b.province,b.city,b.district FROM (SELECT mobile_idfa,et_imp,ip,mobile_app_bundle," +
        "concat_ws('-',dt_y,dt_m,dt_d) AS dt FROM fact_dsp.fact_dsp_imp WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-09-01' " +
        "AND length(mobile_idfa) > 0 AND length(mobile_idfa) != 32 AND concat_ws('-',id,et) " +
        "NOT IN (SELECT concat_ws('-',id,et) FROM fact_dsp.fact_dsp_clk)) AS a LEFT OUTER JOIN tmp.dsp_ip_location b ON a.ip = b.ip")
      .rdd.map(row => (row.getAs[String]("mobile_idfa"),row))


    val activityIdfaEncRDD = spark
      .sql("SELECT a.*,b.country,b.province,b.city,b.district FROM (SELECT mobile_idfa,et_imp,ip,mobile_app_bundle," +
        "concat_ws('-',dt_y,dt_m,dt_d) AS dt FROM fact_dsp.fact_dsp_imp WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-09-01' " +
        "AND length(mobile_idfa) = 32 AND concat_ws('-',id,et) NOT IN (SELECT concat_ws('-',id,et) FROM fact_dsp.fact_dsp_clk)) " +
        "AS a LEFT OUTER JOIN tmp.dsp_ip_location b ON a.ip = b.ip")
      .rdd.map(row => (row.getAs[String]("mobile_idfa"),row))


    (activityImeiRDD,activityImeiEncRDD,activityIdfaRDD,activityIdfaEncRDD)
  }


  /**
    * create data frame of different type of device id and get device id to row pairRDD from dsp clk data
    * @param spark   spark session
    */
  def getClkDataDeviceId2RowPairRDD(spark: SparkSession) = {

    val activityImeiRDD = spark
      .sql("SELECT a.*,b.country,b.province,b.city,b.district FROM (SELECT mobile_imei,et_imp,et,ip,mobile_app_bundle," +
        "concat_ws('-',dt_y,dt_m,dt_d) AS dt FROM fact_dsp.fact_dsp_clk WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-09-01' " +
        "AND length(mobile_imei) > 0 AND length(mobile_imei) != 32) AS a LEFT OUTER JOIN tmp.dsp_ip_location b ON a.ip = b.ip")
      .rdd.map(row => (row.getAs[String]("mobile_imei"),row))


    val activityImeiEncRDD = spark
      .sql("SELECT a.*,b.country,b.province,b.city,b.district FROM (SELECT mobile_imei,et_imp,et,ip,mobile_app_bundle," +
        "concat_ws('-',dt_y,dt_m,dt_d) AS dt FROM fact_dsp.fact_dsp_clk WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-09-01' " +
        "AND length(mobile_imei) = 32) AS a LEFT OUTER JOIN tmp.dsp_ip_location b ON a.ip = b.ip")
      .rdd.map(row => (row.getAs[String]("mobile_imei"),row))


    val activityIdfaRDD = spark
      .sql("SELECT a.*,b.country,b.province,b.city,b.district FROM (SELECT mobile_idfa,et_imp,et,ip,mobile_app_bundle," +
        "concat_ws('-',dt_y,dt_m,dt_d) AS dt FROM fact_dsp.fact_dsp_clk WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-09-01' " +
        "AND length(mobile_idfa) > 0 AND length(mobile_idfa) != 32) AS a LEFT OUTER JOIN tmp.dsp_ip_location b ON a.ip = b.ip")
      .rdd.map(row => (row.getAs[String]("mobile_idfa"),row))


    val activityIdfaEncRDD = spark
      .sql("SELECT a.*,b.country,b.province,b.city,b.district FROM (SELECT mobile_idfa,et_imp,et,ip,mobile_app_bundle," +
        "concat_ws('-',dt_y,dt_m,dt_d) AS dt FROM fact_dsp.fact_dsp_clk WHERE concat_ws('-',dt_y,dt_m,dt_d) < '2018-09-01' " +
        "AND length(mobile_idfa) = 32) AS a LEFT OUTER JOIN tmp.dsp_ip_location b ON a.ip = b.ip")
      .rdd.map(row => (row.getAs[String]("mobile_idfa"),row))


    (activityImeiRDD,activityImeiEncRDD,activityIdfaRDD,activityIdfaEncRDD)
  }


  /**
    * activity data rdd join user info rdd to get lava id and get activity info string to save in hive
    * @param activityRDD    activity rdd include imp data and clk data
    * @param userInfoRDD    device id to lava id rdd from local data base persona user info
    * @param activityType   activity type include imp and clk
    */
  def transferActivityData2HiveRDD(activityRDD:RDD[(String,Row)],
                                   userInfoRDD:RDD[(String,String)],
                                   activityType:String):RDD[String] = {
    activityRDD
      .join(userInfoRDD)
      .map(line => {
        val lava_id = line._2._2
        val imp_date = line._2._1.getAs[String]("et_imp").trim
        val country = line._2._1.getAs[String]("country").trim
        val province = line._2._1.getAs[String]("province").trim
        val city = line._2._1.getAs[String]("city").trim
        val district = line._2._1.getAs[String]("district").trim
        val dt = line._2._1.getAs[String]("dt").trim
        val package_url = line._2._1.getAs[String]("mobile_app_bundle").trim
        if (activityType == "clk"){
          val et = line._2._1.getAs[String]("et").trim
          val clk_date = Json.getClkDateFromJsonString(et)
          lava_id + "," + imp_date + "," + clk_date + ",1,1," + country + "," + province + "," +
            city + "," + district + "," + package_url + "," + dt
        }else {
          lava_id + "," + imp_date + ",-,1,0," + country + "," + province + "," +
            city + "," + district + "," + package_url + "," + dt
        }
      })
  }


  /**
    * transfer activity data in hdfs to persona.activity_dsp in hive
    * @param spark   spark session
    */
  def transferActivityData2Hive(spark: SparkSession): Unit ={

    //create table tmp.dsp_tmp and load activity data
    spark.sql("drop table if exists tmp.dsp_tmp")
    spark.sql("create table tmp.dsp_tmp(" +
      " lava_id STRING COMMENT 'LavaHeat 用户或者设备的唯一标识'," +
      " imp_date STRING COMMENT '展示时间'," +
      " clk_date STRING COMMENT '点击时间'," +
      " is_imp STRING COMMENT '0表示没有展示，1表示有展示'," +
      " is_clk STRING COMMENT '0表示没有点击，1表示有点击'," +
      " country STRING COMMENT '国家'," +
      " province STRING COMMENT '省份'," +
      " city STRING COMMENT '城市'," +
      " district STRING COMMENT '区域'," +
      " package_url STRING COMMENT 'App source name'," +
      " dt STRING COMMENT '日期'" +
      ") ROW FORMAT delimited fields " +
      "terminated by ',' " +
      "lines terminated by '\\n'")

    spark.sql("LOAD DATA INPATH '/user/dmp_app_user/tmp/dsp_imp' INTO TABLE tmp.dsp_tmp")
    spark.sql("LOAD DATA INPATH '/user/dmp_app_user/tmp/dsp_clk' INTO TABLE tmp.dsp_tmp")

    //insert distinct activity data into partition table persona.activity_dsp
    spark.sql("INSERT INTO TABLE persona.activity_dsp PARTITION (dt) SELECT DISTINCT * FROM tmp.dsp_tmp")
  }

}
