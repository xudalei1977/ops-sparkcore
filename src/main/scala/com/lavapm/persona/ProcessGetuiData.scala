package com.lavapm.persona


import com.lavapm.model.Cisco.{originRddCol, schema}
import com.lavapm.utils._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
  * Process Getui Data, extract the data in hdfs file to Hbase table persona:user_info
  * Created by dalei on 3/14/19.
  */
object ProcessGetuiData {


  val originRddCol = Array("mobile_enc", "mobile_brand", "mobile_type", "mobile_price", "mobile_os", "mobile_lang", "mobile_operator", //7
    "province", "city", "age", "gender", "consume_lever", "richman", "household", "marriage", "looking_for_lover", "married", "single_women", //18
    "occupation", "apply_job", "job_hunting", "job_hopping", "teacher", "programmer", "doctor", "truck_driver", "cab_driver", "industry", //28
    "enterprise_owner", "sexual_orientation", "residential_area", "foreign_personnel", "city_level", "house_moving", "domestic_tourism", //35
    "outbound_travel", "has_car", "has_estate", "pre_pregnant", "pregnant", "has_child_0_2", "has_child_3_6", "has_child_0_6", "has_child_3_14", //44
    "has_child_0_14", "is_mother", "mother_baby", "has_pupil", "has_middle", "has_second_child", "has_bady", "graduate",	"stock", //53
    "invest", "keep_account", "bank", "credit", "lottey", "p2p", "online_shopping", "offshore_shopping", //61
    "express", "wine", "group_buying", "pre_school_education", "pri_sed_education", "adult_education", "english_train", "go_aboard_train", //69
    "exam", "language_learn", "online_education", "business_trip", "leisure_travel", "hotel", "outdoor", "around_travel", //77
    "rent_car", "buy_car", "driving_test", "like_car", "driving_service", "peccancy", "car_maintain", "second_car", //85
    "news", "magazine", "novel", "listen_story", "cartoon", "funny", "pictorial", "sports_news", "kara", "music_player", "radio", "ring", //97
    "video_player", "telecast", "online_video", "online_music", "chat", "friends", "dating", "weibo", "community", "restaurant", //107
    "take_out", "fresh_delivery", "cleaner", "washing", "cooking", "spa", "movie", "pet", "rent_house", "buy_house", "decorate", "wedding", //119
    "fitness", "reduce_weight", "health_care",  "chronic", "beautify", "pic_sharing", "video_shooting", "beauty", //127
    "parenting", "menstrual", "taxi",	"menu", "map_nav", "whether", "cloud_disk", "office", "note", "email", "intelligence", "child_edu", //139
    "speed", "sport", "shooting", "cheese", "cosplay", "tactics", "network_game") //146

  val schema = StructType(originRddCol.map(fieldName => StructField(fieldName, StringType, true)))


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                .builder()
                .appName("Process the Getui data")
                .getOrCreate()

    val hdfsPathSrc = "/user/hive/warehouse/tmp.db/getui_user"

    println("******************** before createSourceTableInHive() ")
    //createSourceTableInHive(spark)

    println("******************** before extractGetuiData2UserInfo() ")
    extractGetuiData2UserInfo(spark, hdfsPathSrc)

    spark.stop()
  }


  /**
    * create the hive table to add lava_id column to getui data table
    * @param spark            spark session
    */
  def createSourceTableInHive(spark: SparkSession): Unit = {

    // drop the result table firstly
    spark.sql("drop table if exists tmp.getui_user")

    // create the table
    // spark.cacheTable("sandbox_getui.cisco_pos")
    // spark.cacheTable("sandbox_getui.cisco_neg")

    spark.sql(s"create table tmp.getui_user stored AS parquet as " +
                       s" select a.*, b.lava_id, b.interest_tag from (select * from sandbox_getui.cisco_pos union all select * from sandbox_getui.cisco_neg) a " +
                       s" left outer join (select lava_id, mobile_enc, interest_tag from persona.mobile_enc_interest where mobile_enc is not null and length(trim(mobile_enc)) > 0) b " +
                       s" on a.mobile_md5 = b.mobile_enc")
  }


  /**
    * extract getui data in hive into persona:user_info in hbase
    * @param spark            spark session
    * @param hdfsPathSrc      hdfs path for the getui data
    */
  def extractGetuiData2UserInfo(spark: SparkSession,
                                hdfsPathSrc: String): Unit = {
    try{

      val sc = spark.sparkContext

      val age_map = Map("0_17岁" -> "18-",
                        "18_24岁" -> "19~24",
                        "25_34岁" -> "25~29",
                        "35_44岁" -> "35~39",
                        "45岁以上" -> "40~49")

      val city_level_map = Map("一线城市" -> "1",
                               "二线城市" -> "2",
                               "三线城市" -> "3",
                               "四线及以下城市" -> "4")

      sc.broadcast(age_map)
      sc.broadcast(city_level_map)

      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "persona:user_info")

      lazy val job = Job.getInstance(hbaseConf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

      // read from the hdfs and process each column
      val rdd = spark.read.parquet(hdfsPathSrc).rdd.map { case row =>

        /**
          * check whether the mobile_enc exists in persona:user_info.
          * if yes, this is the same user or device, return the existing lava_id
          * if not, this is a new user or device, create a new lava_id
          */
        // check the lava_id
        var lava_id = ""
        if (row.getAs[String]("lava_id") != null && (! row.getAs[String]("lava_id").trim.equals("")) && (! row.getAs[String]("lava_id").equalsIgnoreCase("NULL")))
          lava_id = row.getAs[String]("lava_id").trim
        else
          lava_id = UUID.generateUUID

        val put = new Put(Bytes.toBytes(lava_id))

        // check the interest_tag
        var interest_tag = ""
        if (row.getAs[String]("interest_tag") != null && (! row.getAs[String]("interest_tag").trim.equals("")))
          interest_tag = row.getAs[String]("interest_tag").trim


        // check the mobile_enc
        if (row.getAs[String]("mobile_md5") != null && (! row.getAs[String]("mobile_md5").trim.equals("")))
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mobile_enc"), Bytes.toBytes(row.getAs[String]("mobile_md5").trim))

        // check the mobile_brand
        if (row.getAs[String]("mobile_brand") != null && (! row.getAs[String]("mobile_brand").trim.equals("")))
          put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes(row.getAs[String]("mobile_brand").trim))

        // check the mobile_os
        if (row.getAs[String]("mobile_os") != null && (! row.getAs[String]("mobile_os").trim.equals("")))
          put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_OS"), Bytes.toBytes(row.getAs[String]("mobile_os").trim))

        // check the mobile_lang
        if (row.getAs[String]("mobile_lang") != null && (! row.getAs[String]("mobile_lang").trim.equals("")))
          put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_language"), Bytes.toBytes(row.getAs[String]("mobile_lang").trim))

        // check the mobile_operator
        if (row.getAs[String]("mobile_operator") != null && (! row.getAs[String]("mobile_operator").trim.equals("")))
          put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_operator"), Bytes.toBytes(row.getAs[String]("mobile_operator").trim))

        // check the province
        if (row.getAs[String]("province") != null && (! row.getAs[String]("province").trim.equals("")))
          put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_province"), Bytes.toBytes(row.getAs[String]("province").trim))

        // check the city
        if (row.getAs[String]("city") != null && (! row.getAs[String]("city").trim.equals("")))
          put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_city"), Bytes.toBytes(row.getAs[String]("city").trim))

        // check the age
        if (row.getAs[String]("age") != null && (! row.getAs[String]("age").trim.equals(""))) {
          if(age_map.contains(row.getAs[String]("age").trim))
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("age_range"), Bytes.toBytes(age_map(row.getAs[String]("age").trim)))
        }

        // check the gender
        if (row.getAs[String]("gender") != null && (! row.getAs[String]("gender").trim.equals(""))) {
          if (row.getAs[String]("gender").trim.equals("男"))
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("gender"), Bytes.toBytes("M"))
          else
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("gender"), Bytes.toBytes("F"))
        }

        // check the consume_lever
        if (row.getAs[String]("consume_lever") != null && (! row.getAs[String]("consume_lever").trim.equals(""))){
          if (row.getAs[String]("consume_lever").trim == "高消费")
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("consume_level"), Bytes.toBytes("H"))
          if (row.getAs[String]("consume_lever").trim == "中消费")
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("consume_level"), Bytes.toBytes("M"))
          if (row.getAs[String]("consume_lever").trim == "低消费")
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("consume_level"), Bytes.toBytes("L"))
        }

        // check the richman
        if (row.getAs[String]("richman") != null && (! row.getAs[String]("richman").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("consume_level"), Bytes.toBytes("H"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_estate"), Bytes.toBytes("1"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_car"), Bytes.toBytes("1"))
        }

        // check the household
        if (row.getAs[String]("household") != null && (! row.getAs[String]("household").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("consume_frequency"), Bytes.toBytes("H"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("online_shopping"), Bytes.toBytes("1"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("offline_shopping"), Bytes.toBytes("1"))
        }

        // check the marriage
        if (row.getAs[String]("marriage") != null && (! row.getAs[String]("marriage").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("marital_status"), Bytes.toBytes(row.getAs[String]("marriage").trim))

        // check the looking_for_lover
        if (row.getAs[String]("looking_for_lover") != null && (! row.getAs[String]("looking_for_lover").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("marital_status"), Bytes.toBytes("未婚"))

        // check the married
        if (row.getAs[String]("married") != null && (! row.getAs[String]("married").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("marital_status"), Bytes.toBytes("已婚"))

        // check the single_women
        if (row.getAs[String]("single_women") != null && (! row.getAs[String]("single_women").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("marital_status"), Bytes.toBytes("未婚"))

        // check the occupation
        if (row.getAs[String]("occupation") != null && (! row.getAs[String]("occupation").trim.equals(""))) {
          if (row.getAs[String]("occupation").trim.equals("白领")) {
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("毕业"))
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("profession"), Bytes.toBytes("白领"))
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("全职工作"))
          } else if (row.getAs[String]("occupation").trim.equals("大学生")) {
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("大学生"))
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("学生"))
          } else if (row.getAs[String]("occupation").trim.equals("中小学生")) {
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("初中生"))
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("学生"))
          }
        }

        // check the apply_job
        if (row.getAs[String]("apply_job") != null && (! row.getAs[String]("apply_job").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("失业"))

        // check the job_hunting
        if (row.getAs[String]("job_hunting") != null && (! row.getAs[String]("job_hunting").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("失业"))

        // check the job_hopping
        if (row.getAs[String]("job_hopping") != null && (! row.getAs[String]("job_hopping").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("全职工作"))

        // check the teacher
        if (row.getAs[String]("teacher") != null && (! row.getAs[String]("teacher").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("profession"), Bytes.toBytes(row.getAs[String]("teacher").trim))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("全职工作"))
        }

        // check the programmer
        if (row.getAs[String]("programmer") != null && (! row.getAs[String]("programmer").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("profession"), Bytes.toBytes(row.getAs[String]("programmer").trim))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("全职工作"))
        }

        // check the doctor
        if (row.getAs[String]("doctor") != null && (! row.getAs[String]("doctor").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("profession"), Bytes.toBytes(row.getAs[String]("doctor").trim))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("全职工作"))
        }

        // check the truck_driver
        if (row.getAs[String]("truck_driver") != null && (! row.getAs[String]("truck_driver").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("profession"), Bytes.toBytes(row.getAs[String]("truck_driver").trim))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("全职工作"))
        }

        // check the cab_driver
        if (row.getAs[String]("cab_driver") != null && (! row.getAs[String]("cab_driver").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("profession"), Bytes.toBytes(row.getAs[String]("cab_driver").trim))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("全职工作"))
        }

        // check the industry
        if (row.getAs[String]("industry") != null && (! row.getAs[String]("industry").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("profession"), Bytes.toBytes(row.getAs[String]("industry").trim))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("全职工作"))
        }

        // check the enterprise_owner
        if (row.getAs[String]("enterprise_owner") != null && (! row.getAs[String]("enterprise_owner").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("profession"), Bytes.toBytes(row.getAs[String]("enterprise_owner").trim))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes("全职工作"))
        }

        // check the city_level
        if (row.getAs[String]("city_level") != null && (! row.getAs[String]("city_level").trim.equals("")))
          if(city_level_map.contains(row.getAs[String]("city_level").trim))
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("city_level"), Bytes.toBytes(city_level_map(row.getAs[String]("city_level").trim)))

        // check the domestic_tourism
        if (row.getAs[String]("domestic_tourism") != null && (! row.getAs[String]("domestic_tourism").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "旅游商旅-国内游"

          if (interest_tag.trim != "" && !interest_tag.contains("旅游商旅-国内游"))
            interest_tag = "旅游商旅-国内游," + interest_tag
        }

        // check the outbound_travel
        if (row.getAs[String]("outbound_travel") != null && (! row.getAs[String]("outbound_travel").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "旅游商旅-出境游"

          if (interest_tag.trim != "" && !interest_tag.contains("旅游商旅-出境游"))
            interest_tag = "旅游商旅-出境游," + interest_tag
        }

        // check the has_car
        if (row.getAs[String]("has_car") != null && (! row.getAs[String]("has_car").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_car"), Bytes.toBytes("1"))

        // check the has_estate
        if (row.getAs[String]("has_estate") != null && (! row.getAs[String]("has_estate").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_estate"), Bytes.toBytes("1"))

        // check the pre_pregnant
        if (row.getAs[String]("pre_pregnant") != null && (! row.getAs[String]("pre_pregnant").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("备孕"))

        // check the pregnant
        if (row.getAs[String]("pregnant") != null && (! row.getAs[String]("pregnant").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("孕期"))

        // check the has_child_0_2
        if (row.getAs[String]("has_child_0_2") != null && (! row.getAs[String]("has_child_0_2").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("家有0-1岁小孩"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
        }

        // check the has_child_3_6
        if (row.getAs[String]("has_child_3_6") != null && (! row.getAs[String]("has_child_3_6").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("家有3-6岁小孩"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
        }

        // check the has_child_0_6
        if (row.getAs[String]("has_child_0_6") != null && (! row.getAs[String]("has_child_0_6").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("家有3-6岁小孩"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
        }

        // check the has_child_3_14
        if (row.getAs[String]("has_child_3_14") != null && (! row.getAs[String]("has_child_3_14").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("家有小学生"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
        }

        // check the has_child_0_14
        if (row.getAs[String]("has_child_0_14") != null && (! row.getAs[String]("has_child_0_14").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("家有小学生"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
        }

        // check the is_mother
        if (row.getAs[String]("is_mother") != null && (! row.getAs[String]("is_mother").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("育儿阶段"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
        }

        // check the mother_baby
        if (row.getAs[String]("mother_baby") != null && (! row.getAs[String]("mother_baby").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("育儿阶段"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
        }

        // check the has_pupil
        if (row.getAs[String]("has_pupil") != null && (! row.getAs[String]("has_pupil").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("家有小学生"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
        }

        // check the has_middle
        if (row.getAs[String]("has_middle") != null && (! row.getAs[String]("has_middle").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("家有初中生"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
        }

        // check the has_second_child
        if (row.getAs[String]("has_second_child") != null && (! row.getAs[String]("has_second_child").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("育儿阶段"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("child_number"), Bytes.toBytes("2"))
        }

        // check the has_bady
        if (row.getAs[String]("has_bady") != null && (! row.getAs[String]("has_bady").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("育儿阶段"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes("1"))
        }

        // check the graduate
        if (row.getAs[String]("graduate") != null && (! row.getAs[String]("graduate").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes("毕业"))

        // check the stock
        if (row.getAs[String]("stock") != null && (! row.getAs[String]("stock").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("financial_activity"), Bytes.toBytes("1"))

          if (interest_tag.trim == "")
            interest_tag = "金融财经-股票"

          if (interest_tag.trim != "" && !interest_tag.contains("金融财经-股票"))
            interest_tag = "金融财经-股票," + interest_tag
        }

        // check the invest
        if (row.getAs[String]("invest") != null && (! row.getAs[String]("invest").trim.equals(""))){
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("financial_activity"), Bytes.toBytes("1"))

          if (interest_tag.trim == "")
            interest_tag = "金融财经-银行产品"

          if (interest_tag.trim != "" && !interest_tag.contains("金融财经-银行产品"))
            interest_tag = "金融财经-银行产品," + interest_tag
        }

        // check the keep_account
        if (row.getAs[String]("keep_account") != null && (! row.getAs[String]("keep_account").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "金融财经-会计审计"

          if (interest_tag.trim != "" && !interest_tag.contains("金融财经-会计审计"))
            interest_tag = "金融财经-会计审计," + interest_tag
        }

        // check the bank
        if (row.getAs[String]("bank") != null && (! row.getAs[String]("bank").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("financial_activity"), Bytes.toBytes("1"))
          if (interest_tag.trim == "")
            interest_tag = "金融财经-银行产品"

          if (interest_tag.trim != "" && !interest_tag.contains("金融财经-银行产品"))
            interest_tag = "金融财经-银行产品," + interest_tag
        }

        // check the credit
        if (row.getAs[String]("credit") != null  && (! row.getAs[String]("credit").trim.equals(""))){
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("financial_activity"), Bytes.toBytes("1"))
          if (interest_tag.trim == "")
            interest_tag = "金融财经-银行产品"

          if (interest_tag.trim != "" && !interest_tag.contains("金融财经-银行产品"))
            interest_tag = "金融财经-银行产品," + interest_tag
        }

        // check the lottey
        if (row.getAs[String]("lottey") != null && (! row.getAs[String]("lottey").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-彩票"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-彩票"))
            interest_tag = "文化娱乐-彩票," + interest_tag
        }

        // check the p2p
        if (row.getAs[String]("p2p") != null && (! row.getAs[String]("p2p").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("financial_activity"), Bytes.toBytes("1"))
          if (interest_tag.trim == "")
            interest_tag = "金融财经-网贷p2p"

          if (interest_tag.trim != "" && !interest_tag.contains("金融财经-网贷p2p"))
            interest_tag = "金融财经-网贷p2p," + interest_tag
        }

        // check the online_shopping
        if (row.getAs[String]("online_shopping") != null && (! row.getAs[String]("online_shopping").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("online_shopping"), Bytes.toBytes("1"))

        // check the offshore_shopping
        if (row.getAs[String]("offshore_shopping") != null && (! row.getAs[String]("offshore_shopping").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("online_shopping"), Bytes.toBytes("1"))

        // check the express
        if (row.getAs[String]("express") != null && (! row.getAs[String]("express").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("online_shopping"), Bytes.toBytes("1"))

        // check the wine
        if (row.getAs[String]("wine") != null && (! row.getAs[String]("wine").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "烟酒"

          if (interest_tag.trim != "" && !interest_tag.contains("烟酒"))
            interest_tag = "烟酒," + interest_tag
        }

        // check the group_buying
        if (row.getAs[String]("group_buying") != null && (! row.getAs[String]("group_buying").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("online_shopping"), Bytes.toBytes("1"))

        // check the pre_school_education
        if (row.getAs[String]("pre_school_education") != null && (! row.getAs[String]("pre_school_education").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "教育培训-幼儿及学前教育"

          if (interest_tag.trim != "" && !interest_tag.contains("教育培训-幼儿及学前教育"))
            interest_tag = "教育培训-幼儿及学前教育," + interest_tag
        }

        // check the pri_sed_education
        if (row.getAs[String]("pri_sed_education") != null && (! row.getAs[String]("pri_sed_education").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "教育培训-中考周边"

          if (interest_tag.trim != "" && !interest_tag.contains("教育培训-中考周边"))
            interest_tag = "教育培训-中考周边," + interest_tag
        }

        // check the adult_education
        if (row.getAs[String]("adult_education") != null && (! row.getAs[String]("adult_education").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "教育培训-职业教育"

          if (interest_tag.trim != "" && !interest_tag.contains("教育培训-职业教育"))
            interest_tag = "教育培训-职业教育," + interest_tag
        }

        // check the english_train
        if (row.getAs[String]("english_train") != null && (! row.getAs[String]("english_train").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "教育培训-外语培训"

          if (interest_tag.trim != "" && !interest_tag.contains("教育培训-外语培训"))
            interest_tag = "教育培训-外语培训," + interest_tag
        }

        // check the go_aboard_train
        if (row.getAs[String]("go_aboard_train") != null && (! row.getAs[String]("go_aboard_train").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "教育培训-留学移民"

          if (interest_tag.trim != "" && !interest_tag.contains("教育培训-留学移民"))
            interest_tag = "教育培训-留学移民," + interest_tag
        }

        // check the exam
        if (row.getAs[String]("exam") != null && (! row.getAs[String]("exam").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "教育培训"

          if (interest_tag.trim != "" && !interest_tag.contains("教育培训"))
            interest_tag = "教育培训," + interest_tag
        }

        // check the language_learn
        if (row.getAs[String]("language_learn") != null && (! row.getAs[String]("language_learn").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "教育培训-外语培训"

          if (interest_tag.trim != "" && !interest_tag.contains("教育培训-外语培训"))
            interest_tag = "教育培训-外语培训," + interest_tag
        }

        // check the online_education
        if (row.getAs[String]("online_education") != null && (! row.getAs[String]("online_education").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "教育培训"

          if (interest_tag.trim != "" && !interest_tag.contains("教育培训"))
            interest_tag = "教育培训," + interest_tag
        }

        // check the business_trip
        if (row.getAs[String]("business_trip") != null && (! row.getAs[String]("business_trip").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "旅游商旅"

          if (interest_tag.trim != "" && !interest_tag.contains("旅游商旅"))
            interest_tag = "旅游商旅," + interest_tag
        }

        // check the leisure_travel
        if (row.getAs[String]("leisure_travel") != null && (! row.getAs[String]("leisure_travel").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "旅游商旅"

          if (interest_tag.trim != "" && !interest_tag.contains("旅游商旅"))
            interest_tag = "旅游商旅," + interest_tag
        }

        // check the hotel
        if (row.getAs[String]("hotel") != null && (! row.getAs[String]("hotel").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "旅游商旅-酒店服务"

          if (interest_tag.trim != "" && !interest_tag.contains("旅游商旅-酒店服务"))
            interest_tag = "旅游商旅-酒店服务," + interest_tag
        }

        // check the outdoor
        if (row.getAs[String]("outdoor") != null && (! row.getAs[String]("outdoor").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "旅游商旅-户外探险"

          if (interest_tag.trim != "" && !interest_tag.contains("旅游商旅-户外探险"))
            interest_tag = "旅游商旅-户外探险," + interest_tag
        }

        // check the around_travel
        if (row.getAs[String]("around_travel") != null && (! row.getAs[String]("around_travel").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "旅游商旅-周边游"

          if (interest_tag.trim != "" && !interest_tag.contains("旅游商旅-周边游"))
            interest_tag = "旅游商旅-周边游," + interest_tag
        }

        // check the rent_car
        if (row.getAs[String]("rent_car") != null && (! row.getAs[String]("rent_car").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "汽车-租车"

          if (interest_tag.trim != "" && !interest_tag.contains("汽车-租车"))
            interest_tag = "汽车-租车," + interest_tag
        }

        // check the buy_car
        if (row.getAs[String]("buy_car") != null && (! row.getAs[String]("buy_car").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "汽车"

          if (interest_tag.trim != "" && !interest_tag.contains("汽车"))
            interest_tag = "汽车," + interest_tag
        }

        // check the driving_test
        if (row.getAs[String]("driving_test") != null && (! row.getAs[String]("driving_test").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "汽车-试驾"

          if (interest_tag.trim != "" && !interest_tag.contains("汽车-试驾"))
            interest_tag = "汽车-试驾," + interest_tag
        }

        // check the like_car
        if (row.getAs[String]("like_car") != null && (! row.getAs[String]("like_car").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "汽车"

          if (interest_tag.trim != "" && !interest_tag.contains("汽车"))
            interest_tag = "汽车," + interest_tag
        }

        // check the driving_service
        if (row.getAs[String]("driving_service") != null && (! row.getAs[String]("driving_service").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "汽车-代驾"

          if (interest_tag.trim != "" && !interest_tag.contains("汽车-代驾"))
            interest_tag = "汽车-代驾," + interest_tag
        }

        // check the peccancy
        if (row.getAs[String]("peccancy") != null && (! row.getAs[String]("peccancy").trim.equals("")))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_car"), Bytes.toBytes("1"))

        // check the car_maintain
        if (row.getAs[String]("car_maintain") != null && (! row.getAs[String]("car_maintain").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_car"), Bytes.toBytes("1"))
          if (interest_tag.trim == "")
            interest_tag = "汽车-美容保养"

          if (interest_tag.trim != "" && !interest_tag.contains("汽车-美容保养"))
            interest_tag = "汽车-美容保养," + interest_tag
        }

        // check the second_car
        if (row.getAs[String]("second_car") != null && (! row.getAs[String]("second_car").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "汽车-二手车"

          if (interest_tag.trim != "" && !interest_tag.contains("汽车-二手车"))
            interest_tag = "汽车-二手车," + interest_tag
        }

        // check the news
        if (row.getAs[String]("news") != null && (! row.getAs[String]("news").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-新闻"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-新闻"))
            interest_tag = "文化娱乐-新闻," + interest_tag
        }

        // check the magazine
        if (row.getAs[String]("magazine") != null && (! row.getAs[String]("magazine").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-报刊杂志"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-报刊杂志"))
            interest_tag = "文化娱乐-报刊杂志," + interest_tag
        }

        // check the novel
        if (row.getAs[String]("novel") != null && (! row.getAs[String]("novel").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-书籍"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-书籍"))
            interest_tag = "文化娱乐-书籍," + interest_tag
        }

        // check the listen_story
        if (row.getAs[String]("listen_story") != null && (! row.getAs[String]("listen_story").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-听书"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-听书"))
            interest_tag = "文化娱乐-听书," + interest_tag
        }

        // check the cartoon
        if (row.getAs[String]("cartoon") != null && (! row.getAs[String]("cartoon").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-漫画"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-漫画"))
            interest_tag = "文化娱乐-漫画," + interest_tag
        }

        // check the funny
        if (row.getAs[String]("funny") != null && (! row.getAs[String]("funny").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-搞笑"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-搞笑"))
            interest_tag = "文化娱乐-搞笑," + interest_tag
        }

        // check the pictorial
        if (row.getAs[String]("pictorial") != null && (! row.getAs[String]("pictorial").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-报刊杂志"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-报刊杂志"))
            interest_tag = "文化娱乐-报刊杂志," + interest_tag
        }

        // check the sports_news
        if (row.getAs[String]("sports_news") != null && (! row.getAs[String]("sports_news").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-体育资讯"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-体育资讯"))
            interest_tag = "文化娱乐-体育资讯," + interest_tag
        }

        // check the kara
        if (row.getAs[String]("kara") != null && (! row.getAs[String]("kara").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-音乐"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-音乐"))
            interest_tag = "文化娱乐-音乐," + interest_tag
        }

        // check the music_player
        if (row.getAs[String]("music_player") != null && (! row.getAs[String]("music_player").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-音乐"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-音乐"))
            interest_tag = "文化娱乐-音乐," + interest_tag
        }

        // check the radio
        if (row.getAs[String]("radio") != null && (! row.getAs[String]("radio").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-音乐"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-音乐"))
            interest_tag = "文化娱乐-音乐," + interest_tag
        }

        // check the ring
        if (row.getAs[String]("ring") != null && (! row.getAs[String]("ring").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-音乐"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-音乐"))
            interest_tag = "文化娱乐-音乐," + interest_tag
        }

        // check the video_player
        if (row.getAs[String]("video_player") != null && (! row.getAs[String]("video_player").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-视频"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-视频"))
            interest_tag = "文化娱乐-视频," + interest_tag
        }

        // check the telecast
        if (row.getAs[String]("telecast") != null && (! row.getAs[String]("telecast").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-视频"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-视频"))
            interest_tag = "文化娱乐-视频," + interest_tag
        }

        // check the online_video
        if (row.getAs[String]("online_video") != null && (! row.getAs[String]("online_video").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-视频"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-视频"))
            interest_tag = "文化娱乐-视频," + interest_tag
        }

        // check the online_music
        if (row.getAs[String]("online_music") != null && (! row.getAs[String]("online_music").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-音乐"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-音乐"))
            interest_tag = "文化娱乐-音乐," + interest_tag
        }

        // check the chat
        if (row.getAs[String]("chat") != null && (! row.getAs[String]("chat").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-聊天"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-聊天"))
            interest_tag = "文化娱乐-聊天," + interest_tag
        }

        // check the friends
        if (row.getAs[String]("friends") != null && (! row.getAs[String]("friends").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-交友"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-交友"))
            interest_tag = "文化娱乐-交友," + interest_tag
        }

        // check the dating
        if (row.getAs[String]("dating") != null && (! row.getAs[String]("dating").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-交友"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-交友"))
            interest_tag = "文化娱乐-交友," + interest_tag
        }

        // check the weibo
        if (row.getAs[String]("weibo") != null && (! row.getAs[String]("weibo").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐"))
            interest_tag = "文化娱乐," + interest_tag
        }

        // check the community
        if (row.getAs[String]("community") != null && (! row.getAs[String]("community").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐"))
            interest_tag = "文化娱乐," + interest_tag
        }

        // check the restaurant
        if (row.getAs[String]("restaurant") != null && (! row.getAs[String]("restaurant").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "食品美食"

          if (interest_tag.trim != "" && !interest_tag.contains("食品美食"))
            interest_tag = "食品美食," + interest_tag
        }

        // check the take_out
        if (row.getAs[String]("take_out") != null && (! row.getAs[String]("take_out").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "食品美食"

          if (interest_tag.trim != "" && !interest_tag.contains("食品美食"))
            interest_tag = "食品美食," + interest_tag
        }

        // check the fresh_delivery
        if (row.getAs[String]("fresh_delivery") != null && (! row.getAs[String]("fresh_delivery").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "食品美食"

          if (interest_tag.trim != "" && !interest_tag.contains("食品美食"))
            interest_tag = "食品美食," + interest_tag
        }

        // check the cleaner
        if (row.getAs[String]("cleaner") != null && (! row.getAs[String]("cleaner").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "家居生活-家政服务"

          if (interest_tag.trim != "" && !interest_tag.contains("家居生活-家政服务"))
            interest_tag = "家居生活-家政服务," + interest_tag
        }

        // check the washing
        if (row.getAs[String]("washing") != null && (! row.getAs[String]("washing").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "家居生活-家政服务"

          if (interest_tag.trim != "" && !interest_tag.contains("家居生活-家政服务"))
            interest_tag = "家居生活-家政服务," + interest_tag
        }

        // check the cooking
        if (row.getAs[String]("cooking") != null && (! row.getAs[String]("cooking").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "家居生活-家政服务"

          if (interest_tag.trim != "" && !interest_tag.contains("家居生活-家政服务"))
            interest_tag = "家居生活-家政服务," + interest_tag
        }

        // check the spa
        if (row.getAs[String]("spa") != null && (! row.getAs[String]("spa").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "健康医疗-保健"

          if (interest_tag.trim != "" && !interest_tag.contains("健康医疗-保健"))
            interest_tag = "健康医疗-保健," + interest_tag
        }

        // check the movie
        if (row.getAs[String]("movie") != null && (! row.getAs[String]("movie").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-电影电视"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-电影电视"))
            interest_tag = "文化娱乐-电影电视," + interest_tag
        }

        // check the pet
        if (row.getAs[String]("pet") != null && (! row.getAs[String]("pet").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "家居生活-宠物及宠物用品"

          if (interest_tag.trim != "" && !interest_tag.contains("家居生活-宠物及宠物用品"))
            interest_tag = "家居生活-宠物及宠物用品," + interest_tag
        }

        // check the rent_house
        if (row.getAs[String]("rent_house") != null && (! row.getAs[String]("rent_house").trim.equals(""))) {
          if (interest_tag.trim == "")
            interest_tag = "房地产-出租"

          if (interest_tag.trim != "" && !interest_tag.contains("房地产-出租"))
            interest_tag = "房地产-出租," + interest_tag
        }

        // check the buy_house
        if (row.getAs[String]("buy_house") != null && (! row.getAs[String]("buy_house").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "房地产-新房"

          if (interest_tag.trim != "" && !interest_tag.contains("房地产-新房"))
            interest_tag = "房地产-新房," + interest_tag
        }

        // check the decorate
        if (row.getAs[String]("decorate") != null && (! row.getAs[String]("decorate").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "房地产-装修"

          if (interest_tag.trim != "" && !interest_tag.contains("房地产-装修"))
            interest_tag = "房地产-装修," + interest_tag
        }

        // check the wedding
        if (row.getAs[String]("wedding") != null && (! row.getAs[String]("wedding").trim.equals(""))) {
          if (interest_tag.trim == "")
            interest_tag = "服务-婚礼"

          if (interest_tag.trim != "" && !interest_tag.contains("服务-婚礼"))
            interest_tag = "服务-婚礼," + interest_tag
        }

        // check the fitness
        if (row.getAs[String]("fitness") != null && (! row.getAs[String]("fitness").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "运动健身"

          if (interest_tag.trim != "" && !interest_tag.contains("运动健身"))
            interest_tag = "运动健身," + interest_tag
        }

        // check the reduce_weight
        if (row.getAs[String]("reduce_weight") != null && (! row.getAs[String]("reduce_weight").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "运动健身-减肥瘦身"

          if (interest_tag.trim != "" && !interest_tag.contains("运动健身-减肥瘦身"))
            interest_tag = "运动健身-减肥瘦身," + interest_tag
        }

        // check the health_care
        if (row.getAs[String]("health_care") != null && (! row.getAs[String]("health_care").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "健康医疗"

          if (interest_tag.trim != "" && !interest_tag.contains("健康医疗"))
            interest_tag = "健康医疗," + interest_tag
        }

        // check the chronic
        if (row.getAs[String]("chronic") != null && (! row.getAs[String]("chronic").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "健康医疗-疾病"

          if (interest_tag.trim != "" && !interest_tag.contains("健康医疗-疾病"))
            interest_tag = "健康医疗-疾病," + interest_tag
        }

        // check the beautify
        if (row.getAs[String]("beautify") != null && (! row.getAs[String]("beautify").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-摄影"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-摄影"))
            interest_tag = "文化娱乐-摄影," + interest_tag
        }

        // check the pic_sharing
        if (row.getAs[String]("pic_sharing") != null && (! row.getAs[String]("pic_sharing").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-图片分享"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-图片分享"))
            interest_tag = "文化娱乐-图片分享," + interest_tag
        }

        // check the video_shooting
        if (row.getAs[String]("video_shooting") != null && (! row.getAs[String]("video_shooting").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "文化娱乐-摄影"

          if (interest_tag.trim != "" && !interest_tag.contains("文化娱乐-摄影"))
            interest_tag = "文化娱乐-摄影," + interest_tag
        }

        // check the beauty
        if (row.getAs[String]("beauty") != null && (! row.getAs[String]("beauty").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "美容及化妆品"

          if (interest_tag.trim != "" && !interest_tag.contains("美容及化妆品"))
            interest_tag = "美容及化妆品," + interest_tag
        }

        // check the parenting
        if (row.getAs[String]("parenting") != null && (! row.getAs[String]("parenting").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "母婴育儿"

          if (interest_tag.trim != "" && !interest_tag.contains("母婴育儿"))
            interest_tag = "母婴育儿," + interest_tag
        }

        // check the menstrual
        if (row.getAs[String]("menstrual") != null && (! row.getAs[String]("menstrual").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "健康医疗"

          if (interest_tag.trim != "" && !interest_tag.contains("健康医疗"))
            interest_tag = "健康医疗," + interest_tag
        }

        // check the taxi
        if (row.getAs[String]("taxi") != null && (! row.getAs[String]("taxi").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "服务-打车出行"

          if (interest_tag.trim != "" && !interest_tag.contains("服务-打车出行"))
            interest_tag = "服务-打车出行," + interest_tag
        }

        // check the menu
        if (row.getAs[String]("menu") != null && (! row.getAs[String]("menu").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "食品美食"

          if (interest_tag.trim != "" && !interest_tag.contains("食品美食"))
            interest_tag = "食品美食," + interest_tag
        }

        // check the map_nav
        if (row.getAs[String]("map_nav") != null && (! row.getAs[String]("map_nav").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "服务-地图导航"

          if (interest_tag.trim != "" && !interest_tag.contains("服务-地图导航"))
            interest_tag = "服务-地图导航," + interest_tag
        }

        // check the whether
        if (row.getAs[String]("whether") != null && (! row.getAs[String]("whether").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "服务-天气查询"

          if (interest_tag.trim != "" && !interest_tag.contains("服务-天气查询"))
            interest_tag = "服务-天气查询," + interest_tag
        }

        // check the cloud_disk
        if (row.getAs[String]("cloud_disk") != null && (! row.getAs[String]("cloud_disk").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "IT及信息产业-互联网存储服务"

          if (interest_tag.trim != "" && !interest_tag.contains("IT及信息产业-互联网存储服务"))
            interest_tag = "IT及信息产业-互联网存储服务," + interest_tag
      }

        // check the office
        if (row.getAs[String]("office") != null && (! row.getAs[String]("office").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "IT及信息产业-软件"

          if (interest_tag.trim != "" && !interest_tag.contains("IT及信息产业-软件"))
            interest_tag = "IT及信息产业-软件," + interest_tag
        }

        // check the note
        if (row.getAs[String]("note") != null && (! row.getAs[String]("note").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "IT及信息产业-软件"

          if (interest_tag.trim != "" && !interest_tag.contains("IT及信息产业-软件"))
            interest_tag = "IT及信息产业-软件," + interest_tag
        }

        // check the email
        if (row.getAs[String]("email") != null && (! row.getAs[String]("email").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "IT及信息产业-软件"

          if (interest_tag.trim != "" && !interest_tag.contains("IT及信息产业-软件"))
            interest_tag = "IT及信息产业-软件," + interest_tag
        }

        // check the intelligence
        if (row.getAs[String]("intelligence") != null && (! row.getAs[String]("intelligence").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "游戏"

          if (interest_tag.trim != "" && !interest_tag.contains("游戏"))
            interest_tag = "游戏," + interest_tag
        }

        // check the child_edu
        if (row.getAs[String]("child_edu") != null && (! row.getAs[String]("child_edu").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "教育培训-幼儿及学前教育"

          if (interest_tag.trim != "" && !interest_tag.contains("教育培训-幼儿及学前教育"))
            interest_tag = "教育培训-幼儿及学前教育," + interest_tag
        }

        // check the speed
        if (row.getAs[String]("speed") != null && (! row.getAs[String]("speed").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "游戏-电子竞技"

          if (interest_tag.trim != "" && !interest_tag.contains("游戏-电子竞技"))
            interest_tag = "游戏-电子竞技," + interest_tag
        }

        // check the sport
        if (row.getAs[String]("sport") != null && (! row.getAs[String]("sport").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "运动健身"

          if (interest_tag.trim != "" && !interest_tag.contains("运动健身"))
            interest_tag = "运动健身," + interest_tag
        }

        // check the shooting
        if (row.getAs[String]("shooting") != null && (! row.getAs[String]("shooting").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "运动健身-射击"

          if (interest_tag.trim != "" && !interest_tag.contains("运动健身-射击"))
            interest_tag = "运动健身-射击," + interest_tag
        }

        // check the cheese
        if (row.getAs[String]("cheese") != null && (! row.getAs[String]("cheese").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "运动健身-棋牌周边"

          if (interest_tag.trim != "" && !interest_tag.contains("运动健身-棋牌周边"))
            interest_tag = "运动健身-棋牌周边," + interest_tag
        }

        // check the cosplay
        if (row.getAs[String]("cosplay") != null && (! row.getAs[String]("cosplay").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "游戏-Cosplay"

          if (interest_tag.trim != "" && !interest_tag.contains("游戏-Cosplay"))
            interest_tag = "游戏-Cosplay," + interest_tag
        }

        // check the network_game
        if (row.getAs[String]("network_game") != null && (! row.getAs[String]("network_game").trim.equals(""))){
          if (interest_tag.trim == "")
            interest_tag = "游戏-网游"

          if (interest_tag.trim != "" && !interest_tag.contains("游戏-网游"))
            interest_tag = "游戏-网游," + interest_tag
        }

        // set the interest_tag and brand_tag
        put.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("brand_tag"), Bytes.toBytes("Cisco"))

        if (interest_tag.trim != "")
          put.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(interest_tag))

        (new ImmutableBytesWritable, put)
      }

      rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
      println("*********** hbase table saved")
    } catch {
      case ex: Exception => {println("*************** ex := " + ex.toString)}
    }
  }



}
