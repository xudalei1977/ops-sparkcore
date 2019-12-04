package com.lavapm.persona

import java.text.{ParseException, SimpleDateFormat}

import com.lavapm.utils._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
  * Process Data from Cisco Web
  * Create by ryan on 03/11/19
  */
object ProcessCiscoWeb {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Process the cisco web data")
      .enableHiveSupport()
      //set hive table dynamic import
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()

    //activity path for saving activity data
    val activityPath_cisco_web = "/user/dmp_app_user/tmp/cisco_web"

    println("******************** before transfer cisco web data to activity data ")
    transferCiscoWeb2ActivityData(spark,activityPath_cisco_web)

    println("******************** before transfer cisco web activity data to hive ")
    transferActivityData2Hive(spark,activityPath_cisco_web)

    println("******************** before transfer cisco web activity data to HBase ")
    transferActivityData2HBase(spark)

    spark.stop()
  }


  /**
    * transfer the cisco web data to activity data and save in hdfs
    * @param spark          spark session
    * @param activityPath   hdfs path for activity path
    */
  def transferCiscoWeb2ActivityData(spark: SparkSession,activityPath:String) = {

    //get the city level map
    val cityLevelMap = Hive.generateCityName2CityLevel(spark)
    spark.sparkContext.broadcast(cityLevelMap)

    //get the ip to location map of distinct ip in table sandbox_cisco.web_his
    val tableName = "sandbox_cisco.web_his"
    val ipLocationMap = Hive.generateIp2LocationMap(spark,tableName)
    spark.sparkContext.broadcast(ipLocationMap)

    //select necessary fields from hive table sandbox_cisco.web_his left outer join persona.user_mobile_enc and create data frame
    val df = spark
      .sql("SELECT b.lava_id,a.mailaddress_enc,a.busphone_enc,a.ip,a.viewdate,a.datetime,a.flag_search,a.flag_download,a.architecture," +
        "a.category_1,a.category_2 FROM sandbox_cisco.web_his a left outer join persona.user_mobile_enc b on a.busphone_enc = b.mobile_enc")

    //process df to hiveRDD for saving in hdfs next
    val hiveRDD = df.rdd
      .map(row => {
        var lava_id, email_enc, mobile_enc, country, province, city, city_level, district, ip_address, activity_date,
        activity_time, is_search, is_download, architecture, category_1, category_2, dt = ""

        if (row.getAs[String]("lava_id") != null) {
          lava_id = row.getAs[String]("lava_id").trim
        } else {
          lava_id = UUID.generateUUID()
        }

        if (row.getAs[String]("mailaddress_enc") != null && row.getAs[String]("mailaddress_enc").trim != "")
          email_enc = row.getAs[String]("mailaddress_enc").trim

        if (row.getAs[String]("busphone_enc") != null && row.getAs[String]("busphone_enc").trim != "")
          mobile_enc = row.getAs[String]("busphone_enc").trim

        if (row.getAs[String]("ip") != null && row.getAs[String]("ip").trim != ""){
          ip_address = row.getAs[String]("ip").trim
          if (ipLocationMap.contains(ip_address)){
            val tuple4 = ipLocationMap.getOrElse(ip_address,("","","",""))
            country = tuple4._1
            province = tuple4._2
            city = tuple4._3
            district = tuple4._4

            if (cityLevelMap.contains(city))
              city_level = cityLevelMap.get(city).mkString
          }
        }

        try {
          if (row.getAs[String]("viewdate") != null && row.getAs[String]("viewdate").trim != ""){
            val time = row.getAs[String]("viewdate").trim
            val date = new SimpleDateFormat("yyyy/M/d").parse(time)
            activity_date = new SimpleDateFormat("yyyy-MM-dd").format(date)
            dt = activity_date
          }

          if (row.getAs[String]("datetime") != null && row.getAs[String]("datetime").trim != ""){
            var time = row.getAs[String]("datetime").trim
            if (time.indexOf(" ") > 0){
              time = time.split(" ")(1)
              val date = new SimpleDateFormat("H:m").parse(time)
              activity_time = new SimpleDateFormat("HH:mm").format(date)
            }
          }
        } catch {
          case ex: ParseException => {println("*************** Java.Text.ParseException := " + ex.toString)}
        }

        if (row.getAs[String]("flag_search") != null && row.getAs[String]("flag_search").trim != "")
          is_search = row.getAs[String]("flag_search").trim

        if (row.getAs[String]("flag_download") != null && row.getAs[String]("flag_download").trim != "")
          is_download = row.getAs[String]("flag_download").trim

        if (row.getAs[String]("architecture") != null && row.getAs[String]("architecture").trim != "")
          architecture = row.getAs[String]("architecture").trim

        if (row.getAs[String]("category_1") != null && row.getAs[String]("category_1").trim != "")
          category_1 = row.getAs[String]("category_1").trim

        if (row.getAs[String]("category_2") != null && row.getAs[String]("category_2").trim != "")
          category_2 = row.getAs[String]("category_2").trim

        lava_id + "," + email_enc + "," + mobile_enc + "," + country + "," + province + "," + city + "," + city_level + "," +
          district + "," + ip_address + "," + activity_date + "," + activity_time + "," + is_search + "," + is_download + "," +
          architecture + "," + category_1 + "," + category_2 + "," + dt
      })

    // delete the target hdfs path if exists
    Hdfs.deleteFile(spark, activityPath)
    hiveRDD.saveAsTextFile(activityPath)
    println("******************************* after saving as text file")

  }


  /**
    * transfer activity data in hdfs to persona.activity_cisco_web in hive
    * @param spark          spark session
    * @param activityPath   hdfs path for activity data
    */
  def transferActivityData2Hive(spark: SparkSession,activityPath:String) = {

    //create table tmp.cisco_tmp and load activity data
    spark.sql("drop table if exists tmp.cisco_tmp")
    spark.sql("create table tmp.cisco_tmp(" +
      "lava_id STRING COMMENT 'LavaHEAT 用户或者设备的唯一标识'," +
      "email_enc STRING COMMENT '加密的邮箱地址'," +
      "mobile_enc STRING COMMENT '加密的手机号'," +
      "country STRING COMMENT '国家'," +
      "province STRING COMMENT '省份'," +
      "city STRING COMMENT '城市'," +
      "city_level STRING COMMENT '城市等级'," +
      "district STRING COMMENT '区域'," +
      "ip_address STRING COMMENT 'ip地址信息'," +
      "activity_date STRING COMMENT '访问网站的日期'," +
      "activity_time STRING COMMENT '访问网站的时间'," +
      "is_search STRING COMMENT '是否有过搜索行为，Yes表示是'," +
      "is_download STRING COMMENT '是否有过下载行为，Yes表示是'," +
      "architecture STRING COMMENT '关注的架构'," +
      "category_1 STRING COMMENT '关注的一级类别'," +
      "category_2 STRING COMMENT '关注的二级类别'," +
      "dt STRING COMMENT '日期'" +
      ") COMMENT 'Web历史记录临时表' " +
      "ROW FORMAT delimited fields " +
      "terminated by ',' " +
      "lines terminated by '\\n'")

    //load activity data to table tmp.cisco_tmp
    spark.sql("LOAD DATA INPATH '" + activityPath + "' INTO TABLE tmp.cisco_tmp")

    //distinct lava_id for the same mobile_enc
    spark.sql("drop table if exists tmp.ciscoweb_lavaid_distinct")
    spark.sql("create table tmp.ciscoweb_lavaid_distinct stored AS parquet as select max(lava_id) as lava_id,mobile_enc " +
      "from tmp.cisco_tmp group by mobile_enc")

    //refresh the table tmp.cisco_tmp
    spark.sql("INSERT OVERWRITE TABLE tmp.cisco_tmp SELECT a.lava_id,b.email_enc,b.mobile_enc,b.country,b.province," +
      "b.city,b.city_level,b.district,b.ip_address,b.activity_date,b.activity_time,b.is_search,b.is_download,b.architecture," +
      "b.category_1,b.category_2,b.dt FROM tmp.ciscoweb_lavaid_distinct a,tmp.cisco_tmp b WHERE a.mobile_enc = b.mobile_enc")

    //create partitioned table persona.activity_cisco_web and insert tmp.cisco_tmp data to it
    spark.sql("drop table if exists persona.activity_cisco_web")
    spark.sql("create table persona.activity_cisco_web(" +
      "lava_id STRING COMMENT 'LavaHEAT 用户或者设备的唯一标识'," +
      "email_enc STRING COMMENT '加密的邮箱地址'," +
      "mobile_enc STRING COMMENT '加密的手机号'," +
      "country STRING COMMENT '国家'," +
      "province STRING COMMENT '省份'," +
      "city STRING COMMENT '城市'," +
      "city_level STRING COMMENT '城市等级'," +
      "district STRING COMMENT '区域'," +
      "ip_address STRING COMMENT 'ip地址信息'," +
      "activity_date STRING COMMENT '访问网站的日期'," +
      "activity_time STRING COMMENT '访问网站的时间'," +
      "is_search STRING COMMENT '是否有过搜索行为，Yes表示是'," +
      "is_download STRING COMMENT '是否有过下载行为，Yes表示是'," +
      "architecture STRING COMMENT '关注的架构'," +
      "category_1 STRING COMMENT '关注的一级类别'," +
      "category_2 STRING COMMENT '关注的二级类别'" +
      ") COMMENT 'Web历史记录表' " +
      "PARTITIONED BY (dt string COMMENT '以天分区：yyyy-mm-dd') " +
      "ROW FORMAT delimited fields " +
      "terminated by '\\t' " +
      "lines terminated by '\\n'")

    //insert activity data into final table persona.activity_cisco_web
    spark.sql("INSERT INTO TABLE persona.activity_cisco_web PARTITION (dt) SELECT DISTINCT * FROM tmp.cisco_tmp")
  }



  /**
    * transfer persona.activity_cisco_web to persona:user_info in HBase
    * @param spark   spark session
    */
  def transferActivityData2HBase(spark: SparkSession): Unit = {

    try{
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "persona:user_info")

      lazy val job = Job.getInstance(hbaseConf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

      //create cisco web data frame
      val df = spark.sql("select lava_id,email_enc,mobile_enc,country,province,city,city_level," +
        "district,ip_address,is_search,is_download from persona.activity_cisco_web")

      //process the data
      val hbaseRDD = df.rdd
        .map(row => {
          val lava_id = row.getAs[String]("lava_id")
          val put = new Put(Bytes.toBytes(lava_id))

          // check the email_enc
          if (row.getAs[String]("email_enc").trim != "")
            put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("email_enc"), Bytes.toBytes(row.getAs[String]("email_enc").trim))

          // check the mobile_enc
          if (row.getAs[String]("mobile_enc").trim != "")
            put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mobile_enc"), Bytes.toBytes(row.getAs[String]("mobile_enc").trim))

          // check the country
          if (row.getAs[String]("country").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_country"), Bytes.toBytes(row.getAs[String]("country").trim))

          // check the province
          if (row.getAs[String]("province").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_province"), Bytes.toBytes(row.getAs[String]("province").trim))

          // check the city
          if (row.getAs[String]("city").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_city"), Bytes.toBytes(row.getAs[String]("city").trim))

          // check the city_level
          if (row.getAs[String]("city_level").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("city_level"), Bytes.toBytes(row.getAs[String]("city_level").trim))

          // check the district
          if (row.getAs[String]("district").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_district"), Bytes.toBytes(row.getAs[String]("district").trim))

          // check the ip_address
          if (row.getAs[String]("ip_address").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("ip_address"), Bytes.toBytes(row.getAs[String]("ip_address").trim))

          // check the source_system
          if (row.getAs[String]("is_search").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("is_search"), Bytes.toBytes(row.getAs[String]("is_search").trim))

          // check the regist_flag
          if (row.getAs[String]("is_download").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("is_download"), Bytes.toBytes(row.getAs[String]("is_download").trim))

          // add the interested brand_tag
          put.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("brand_tag"), Bytes.toBytes("Cisco"))

          (new ImmutableBytesWritable, put)
        })

      hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration())
      println("*********** hbase table saved")
    } catch {
      case ex: Exception => {println("*************** ex := " + ex.toString)}
    }
  }

}

