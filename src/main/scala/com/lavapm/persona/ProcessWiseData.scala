package com.lavapm.persona

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import com.lavapm.utils._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}


/**
  * Process Data from wisemedia, including Kanglebao, NBA, .etc.
  * Created by dalei on 11/15/18.
  * Modified by ryan on 12/25/18.
  */

object ProcessWiseData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Process the wise media data")
      .enableHiveSupport()
      //set hive table dynamic import
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()

    val activityPath_klb_imei = "/user/dmp_app_user/tmp/kanglebao/kanglebao_imei"
    val activityPath_klb_imei_enc = "/user/dmp_app_user/tmp/kanglebao/kanglebao_imei_enc"
    val activityPath_klb_idfa = "/user/dmp_app_user/tmp/kanglebao/kanglebao_idfa"

    val activityPath_nba_imei = "/user/dmp_app_user/tmp/nba/nba_imei"
    val activityPath_nba_imei_enc = "/user/dmp_app_user/tmp/nba/nba_imei_enc"
    val activityPath_nba_idfa = "/user/dmp_app_user/tmp/nba/nba_idfa"

    val deviceType_imei = "imei"
    val deviceType_imei_enc = "imei_enc"
    val deviceType_idfa = "idfa"

    val dataType_klb = "kanglebao"
    val dataType_nba = "nba"

    // create the data format for the transfer process
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    // get the map from city name to city level for the transfer process
    val cityLevelMap = Hive.generateCityName2CityLevel(spark)
    spark.sparkContext.broadcast(cityLevelMap)

    //println("**************** before divide kanglebao data into 3 parts")
    //divideDataInto3Parts(spark,dataType_klb)

    println("**************** before transfer klb data where the device_id type is imei")
    //transferData2ActivityData(spark,activityPath_klb_imei,deviceType_imei,dataType_klb,dateFormat,cityLevelMap)

    println("**************** before transfer klb data where the device_id type is imei_enc")
    //transferData2ActivityData(spark,activityPath_klb_imei_enc,deviceType_imei_enc,dataType_klb,dateFormat,cityLevelMap)

    println("**************** before transfer klb data where the device_id type is idfa")
    //transferData2ActivityData(spark,activityPath_klb_idfa,deviceType_idfa,dataType_klb,dateFormat,cityLevelMap)

    println("**************** before transfer klb activity data to Hive")
    //transferActivityData2Hive(spark,dataType_klb)

    println("**************** before transfer klb activity data to HBase")
    //transferActivityData2HBase(spark,dataType_klb)

    println("***************************NBA****************************")

    println("**************** before divide nba data into 3 parts")
    //divideDataInto3Parts(spark,dataType_nba)

    println("**************** before transfer nba data where the device_id type is imei")
    //transferData2ActivityData(spark,activityPath_nba_imei,deviceType_imei,dataType_nba,dateFormat,cityLevelMap)

    println("**************** before transfer nba data where the device_id type is imei_enc")
    //transferData2ActivityData(spark,activityPath_nba_imei_enc,deviceType_imei_enc,dataType_nba,dateFormat,cityLevelMap)

    println("**************** before transfer nba data where the device_id type is idfa")
    //transferData2ActivityData(spark,activityPath_nba_idfa,deviceType_idfa,dataType_nba,dateFormat,cityLevelMap)

    println("**************** before transfer nba activity data to Hive")
    //transferActivityData2Hive(spark,dataType_nba)

    println("**************** before transfer nba activity data to HBase")
    transferActivityData2HBase(spark,dataType_nba)

    spark.stop
  }


  /**
    * divide the data into 3 parts, this is because hiveContext does not allow "or" filter
    * @param spark       spark session
    */
  def divideDataInto3Parts(spark: SparkSession,dataType:String): Unit = {

    // separate the data to 3 parts according to its device id: idfa, imei, imei_enc
    spark.sql("drop table if exists tmp." + dataType + "_idfa")
    spark.sql("create table tmp." + dataType + "_idfa stored AS parquet as select * from sandbox_wisemedia." + dataType + "" +
      " where length(device_id) > 32")

    spark.sql("drop table if exists tmp." + dataType + "_imei")
    spark.sql("create table tmp." + dataType + "_imei stored AS parquet as select * from sandbox_wisemedia." + dataType + "" +
      " where length(device_id) < 32")

    spark.sql("drop table if exists tmp." + dataType + "_imei_enc")
    spark.sql("create table tmp." + dataType + "_imei_enc stored AS parquet as select * from sandbox_wisemedia." + dataType + "" +
      " where length(device_id) = 32")
  }


  /**
    * transfer the data to activity data and save in hdfs
    * @param spark           spark session
    * @param activityPath    hdfs path for the activity data
    * @param deviceType      device type of device_id
    * @param dateFormat      format the imp_date or clk_date to "yyyy-MM-dd"
    * @param cityLevelMap         the map from city name to city level
    */
  def transferData2ActivityData(spark: SparkSession,
                                activityPath: String,
                                deviceType:String,
                                dataType:String,
                                dateFormat: SimpleDateFormat,
                                cityLevelMap:Map[String,String]): Unit = {
    // create data frame, check if the device_id is already exits in our database
    // and join tmp.kanglebao_location or tmp.nba_location to get location message
    val dsRow = spark.sql("SELECT c.*,d.lava_id from (SELECT a.*,b.country,b.province,b.city,b.district " +
      "from tmp." + dataType + "_" + deviceType + " a,tmp." + dataType + "_location b WHERE a.ip_address = b.ip_address) c " +
      "left outer join persona.user_" + deviceType + " d on c.device_id = d." + deviceType + " ")

    // process the data
    val dsStr = dsRow.rdd.map { row => {
      var is_imp, is_clk = "0"
      var lava_id, imp_date, clk_date, ip_address, dt, country, province, city, city_level, district, package_url, device_id = ""

      if (row.getAs[String]("lava_id") != null) {
        lava_id = row.getAs[String]("lava_id").trim
      } else {
        lava_id = UUID.generateUUID()
      }

      device_id = row.getAs[String]("device_id").trim
      imp_date = row.getAs[String]("imp_date").trim
      clk_date = row.getAs[String]("clk_date").trim
      ip_address = row.getAs[String]("ip_address").trim
      package_url = row.getAs[String]("package_url").trim

      if (row.getAs[String]("country") != null)
        country = row.getAs[String]("country").trim
      if (row.getAs[String]("province") != null)
        province = row.getAs[String]("province").trim
      if (row.getAs[String]("city") != null)
        city = row.getAs[String]("city").trim
      if (row.getAs[String]("district") != null)
        district = row.getAs[String]("district").trim

      if (cityLevelMap.contains(city))
        city_level = cityLevelMap.get(city).mkString.trim

      if (imp_date.length > 5) {
        is_imp = "1"
        dt = dateFormat.format((imp_date.toLong) * 1000)
      }

      if (clk_date.length > 5) {
        is_clk = "1"
        dt = dateFormat.format((clk_date.toLong) * 1000)
      }

      lava_id + "," + imp_date + "," + clk_date + "," + is_imp + "," + is_clk + "," + country + "," + province + "," +
        city + "," + city_level + "," + district + "," + package_url + "," + device_id + "," + ip_address + "," + dt

    }
    }

    // delete the target hdfs path if exists
    Hdfs.deleteFile(spark, activityPath)

    dsStr.saveAsTextFile(activityPath)
    println("******************************* after saving as text file")

  }

  /**
    * transfer activity data in hdfs to persona.activity_kanglebao or persona.activity_nba in hive
    * @param spark      spark session
    * @param dataType   date type include kanglebao and nba
    */
  def transferActivityData2Hive(spark: SparkSession,dataType:String): Unit ={

    //create table tmp.kanglebao_tmp or tmp.nba_tmp and load activity data
    spark.sql("drop table if exists tmp." + dataType + "_tmp")
    spark.sql("create table tmp." + dataType + "_tmp(" +
      " lava_id STRING COMMENT 'LavaHeat 用户或者设备的唯一标识'," +
      " imp_date STRING COMMENT '展示时间'," +
      " clk_date STRING COMMENT '点击时间'," +
      " is_imp STRING COMMENT '0表示没有展示，1表示有展示'," +
      " is_clk STRING COMMENT '0表示没有点击，1表示有点击'," +
      " country STRING COMMENT '国家'," +
      " province STRING COMMENT '省份'," +
      " city STRING COMMENT '城市'," +
      " city_level STRING COMMENT '城市等级'," +
      " district STRING COMMENT '区域'," +
      " package_url STRING COMMENT 'App source name'," +
      " device_id STRING COMMENT '设备号'," +
      " ip_address STRING COMMENT 'ip地址信息'," +
      " dt STRING COMMENT '日期'" +
      ") ROW FORMAT delimited fields " +
      "terminated by ',' " +
      "lines terminated by '\\n'")

    spark.sql("LOAD DATA INPATH '/user/dmp_app_user/tmp/" + dataType + "/" + dataType + "_imei' INTO TABLE tmp." + dataType + "_tmp")
    spark.sql("LOAD DATA INPATH '/user/dmp_app_user/tmp/" + dataType + "/" + dataType + "_imei_enc' INTO TABLE tmp." + dataType + "_tmp")
    spark.sql("LOAD DATA INPATH '/user/dmp_app_user/tmp/" + dataType + "/" + dataType + "_idfa' INTO TABLE tmp." + dataType + "_tmp")

    //a single device_id may has several imp or clk records, therefore multiple lava_id were created for it ,so distinct it
    spark.sql("drop table if exists tmp." + dataType + "_lavaid_distinct")
    spark.sql("create table tmp." + dataType + "_lavaid_distinct stored AS parquet as select max(lava_id) as lava_id,device_id " +
      "from tmp." + dataType + "_tmp group by device_id")

    //reload activity data that the same device_id has its single lava_id
    spark.sql("INSERT OVERWRITE TABLE tmp." + dataType + "_tmp SELECT a.lava_id,b.imp_date,b.clk_date,b.is_imp," +
      "b.is_clk,b.country,b.province,b.city,b.city_level,b.district,b.package_url,b.device_id,b.ip_address,b.dt " +
      "FROM tmp." + dataType + "_lavaid_distinct a,tmp." + dataType + "_tmp b WHERE a.device_id = b.device_id")

    spark.sql("drop table if exists persona.activity_" + dataType + "")
    spark.sql("create table persona.activity_" + dataType + "(" +
      " lava_id STRING COMMENT 'LavaHeat 用户或者设备的唯一标识'," +
      " imp_date STRING COMMENT '展示时间'," +
      " clk_date STRING COMMENT '点击时间'," +
      " is_imp STRING COMMENT '0表示没有展示，1表示有展示'," +
      " is_clk STRING COMMENT '0表示没有点击，1表示有点击'," +
      " country STRING COMMENT '国家'," +
      " province STRING COMMENT '省份'," +
      " city STRING COMMENT '城市'," +
      " city_level STRING COMMENT '城市等级'," +
      " district STRING COMMENT '区域'," +
      " package_url STRING COMMENT 'App source name'," +
      " device_id STRING COMMENT '设备号'," +
      " ip_address STRING COMMENT 'ip地址信息'" +
      ") COMMENT '"+ dataType +"人群行为表' " +
      "PARTITIONED BY (dt STRING COMMENT '以天分区：yyyy-mm-dd') " +
      "ROW FORMAT delimited fields " +
      "terminated by ',' " +
      "lines terminated by '\\n'")
    //insert distinct activity data into final table persona.activity_kanglebao or persona.activity_nba
    spark.sql("INSERT INTO TABLE persona.activity_" + dataType + " PARTITION (dt) SELECT DISTINCT * " +
      "FROM tmp." + dataType + "_tmp WHERE country != ''")
  }


  /**
    * transfer persona.activity_kanglebao or persona.activity_nba in hive to persona:user_info in HBase
    * @param spark          spark session
    * @param dataType       data type include kanglebao and nba
    */
  def transferActivityData2HBase(spark: SparkSession,dataType:String): Unit = {

    try{
      // get the map from package name to interest
      val packageInterestMap = Hive.generateApp2InterestMap(spark)
      spark.sparkContext.broadcast(packageInterestMap)
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "persona:user_info")

      lazy val job = Job.getInstance(hbaseConf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

      //create activity data frame and cache
      val df = spark.sql("select * from persona.activity_" + dataType + "").cache()

      println("**************** before getLavaID2InterestFromActivityData")
      val activityInterestRDD = getLavaID2InterestFromActivityData(spark,df,packageInterestMap)

      println("**************** before getLavaID2InterestFromUserInfoData")
      val userInfoInterestRDD = Hive.transferInterestFromUserInfo2UserInterestRDD(spark)

      println("**************** before combineInterestsOfActivityAndUserInfo")
      val interestRDD = combineInterestsOfActivityAndUserInfo(activityInterestRDD,userInfoInterestRDD)

      //process the data
      val hbaseRDD = df.rdd
        .map(row => (row.getAs[String]("lava_id").trim,row))
        .join(interestRDD)
        .map(line => {
          val lava_id = line._1
          val put = new Put(Bytes.toBytes(lava_id))

          val device_id = line._2._1.getAs[String]("device_id").trim

          //check the device_id
          if (device_id.length > 32) { // the device id is an idfa
            put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("idfa"), Bytes.toBytes(device_id))
            put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes("iPhone"))
            put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_os"), Bytes.toBytes("iOS"))
          }

          if (device_id.length < 32) { // the device id is an imei
            put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("imei"), Bytes.toBytes(device_id))
          }

          if (device_id.length == 32){ // the device id is an imei_enc
            put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("imei_enc"), Bytes.toBytes(device_id))
          }

          // check the country
          if (line._2._1.getAs[String]("country").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_country"), Bytes.toBytes(line._2._1.getAs("country").toString.trim))

          // check the province
          if (line._2._1.getAs[String]("province").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_province"), Bytes.toBytes(line._2._1.getAs("province").toString.trim))

          // check the city
          if (line._2._1.getAs[String]("city").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_city"), Bytes.toBytes(line._2._1.getAs("city").toString.trim))

          // check the city_level
          if (line._2._1.getAs[String]("city_level").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("city_level"), Bytes.toBytes(line._2._1.getAs("city_level").toString.trim))

          // check the district
          if (line._2._1.getAs[String]("district").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_district"), Bytes.toBytes(line._2._1.getAs("district").toString.trim))

          // check the ip_address
          if (line._2._1.getAs[String]("ip_address").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("ip_address"), Bytes.toBytes(line._2._1.getAs("ip_address").toString.trim))

          //  check the interest_tag
          if (dataType == "kanglebao") {
            if (line._2._2 != null && line._2._2 != "")
              put.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes("健康医疗,健康医疗-保健器材," + line._2._2))
            else
              put.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes("健康医疗,健康医疗-保健器材"))
          } else {
            if (line._2._2 != null && line._2._2 != "")
              put.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes("运动健身,运动健身-篮球运动周边," + line._2._2))
            else
              put.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes("运动健身,运动健身-篮球运动周边"))
          }

          (new ImmutableBytesWritable, put)
        })
      df.unpersist()
      hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration())
      println("*********** hbase table saved")
    } catch {
      case ex: Exception => {println("*************** ex := " + ex.toString)}
    }

  }


  /**
    * get lava_id to interest pairRDD from activity data and reduce interests of same lava_id
    * @param spark                 spark session
    * @param df                    activity data set
    * @param packageInterestMap    map from package name to interest
    */
  def getLavaID2InterestFromActivityData(spark:SparkSession,
                                         df:Dataset[Row],
                                         packageInterestMap:Map[String,String]):RDD[(String,String)]={

    val activityInterestRDD = df.rdd
      .map(row => {

        if (packageInterestMap.contains(row.getAs("package_url").toString.trim)) {
          var interest = packageInterestMap.get(row.getAs("package_url").toString.trim).mkString

          if (interest.indexOf("-") > 0) {
            interest = interest.split("-")(0) + "," + interest
          }

          (row.getAs[String]("lava_id").trim, interest)
        } else {
          (row.getAs[String]("lava_id").trim, "")
        }

      })
      //reduce interests of same lava_id
      .reduceByKey( _ + "," + _)
      //transfer multiple "," to single ","
      .map(line => {
      val regEx = "[',']+"
      val pattern = Pattern.compile(regEx)
      val matcher = pattern.matcher(line._2)
      var value = matcher.replaceAll(",")
      if (value.length > 0 && value.charAt(0) == ',')
        value = value.replaceFirst(",","")

      (line._1,value)
    })

    activityInterestRDD
  }


  /**
    * combine the interests of activity data and user info data
    * @param activityInterestRDD        interest of activity data
    * @param userInfoInterestRDD        interest of user info data
    */
  def combineInterestsOfActivityAndUserInfo(activityInterestRDD:RDD[(String,String)],
                                            userInfoInterestRDD:RDD[(String,String)]):RDD[(String,String)] ={

    val interestRDD = activityInterestRDD
      .leftOuterJoin(userInfoInterestRDD)
      .map(line => {
        if (line._2._1 != ""){
          if (line._2._2 == None || line._2._2.mkString == "")
            (line._1, line._2._1)
          else
            (line._1, (line._2._1 + "," + line._2._2.mkString))

        }else{
          if (line._2._2 == None || line._2._2.mkString == "")
            (line._1, "")
          else
            (line._1, line._2._2.mkString)
        }
      })

    interestRDD

  }

}
