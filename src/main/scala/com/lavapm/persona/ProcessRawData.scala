package com.lavapm.persona

import com.lavapm.dmp.Test.hbaseConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import com.lavapm.utils._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SparkSession



/**
  * Transfer the raw data from hive to hbase.
  * Created by dalei on 6/20/18.
  */

object ProcessRawData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                .builder()
                .appName("Process the Raw data")
                .getOrCreate()

    val rawColumn = Array("lava_id", "mobile", "name", "name_en", "idno", "gender", "age", "age_range",                   //8
                          "birthday", "nation", "birth_season", "zodiac", "nationality", "birth_place",                   //14
                          "mobile_location", "mobile_duration", "bank_no", "driving_license", "email",                    //19
                          "residential_province", "residential_city", "residential_address",                              //22
                          "nearest_plaza", "nearest_plaza_distance", "shopping_zone", "daily_activity_circle",            //26
                          "profession", "professional_title", "working_status", "working_years",                          //30
                          "industry_type", "education_status", "highest_education", "graduate_school", "graduate_year",   //35
                          "major",  "mastered_language", "marital_status", "has_child",                                   //39
                          "yearly_income_range", "income_source", "life_stage", "financial_situation",                    //43
                          "offline_shopping", "offline_entertainment", "financial_activity", "online_activity",           //47
                          "mac", "mac_enc", "imei", "imei_enc", "idfa", "idfa_enc",                                       //53
                          "id", "prephonenumber", "provience", "city", "operator", "city_level")                          //59

    val basicColumn = Array("lava_id", "mobile", "name", "name_en", "id_no", "gender", "age", "age_range",
                            "birthday", "nation", "birth_season", "zodiac", "nationality", "life_stage",
                            "profession", "profession_title", "working_status", "working_years", "company",
                            "industry_type", "education_status", "highest_education", "graduate_school", "graduate_year",
                            "major", "foreign_language", "marital_status", "has_child", "child_number",
                            "yearly_income_range", "income_source", "has_estate", "has_car", "consume_level",
                            "consume_frequency", "offline_shopping", "offline_entertainment", "financial_activity",
                            "online_activity")

    val deviceColumn = Array("mobile_operator", "mobile_brand", "mobile_language", "mobile_os", "mobile_size",
                            "laptop_brand", "laptop_language", "laptop_os", "laptop_browser",
                            "other_brand", "other_language", "other_os", "other_size")

    val idColumn = Array("bank_no", "driving_license", "passport", "email", "mac", "mac_enc", "imei", "imei_enc",
                        "idfa", "idfa_enc", "qq", "wechat", "baidu_id", "taobao_id", "weibo_id",
                        "linkin_id", "google_id", "facebook_id", "instagram_id", "twitter_id", "cookie_id", "adx_id")

    val activityColumn = Array("mobile_duration", "mobile_status", "bank_no_active", "email_active", "mac_active", "imei_active",
                              "idfa_active", "qq_active", "wechat_active", "baidu_active", "taobao_active", "weibo_active", "linkin_active",
                              "google_active", "facebook_active", "instagram_active", "twitter_active", "cookie_active", "adx_active")

    val geoColumn = Array("birth_province", "birth_city", "mobile_province", "mobile_city", "resident_country", "resident_province",
                          "resident_city", "resident_district", "resident_address", "city_level", "ip_address", "landmark",
                          "landmark_distance", "landmark_times_30", "landmark_times_180", "landmark_last_time", "landmark_duration",
                          "weekly_gathering_place", "monthly_gathering_place", "latitude_longitude", "trip_country", "trip_city")

    val hdfsPathSrc = "/user/hive/warehouse/tmp.db/user_basic_info"

    println("**************** before createSourceTableInHive() ")
    createSourceTableInHive(spark)

    println("**************** before createTableInHBase() ")
    createTableInHBase

    println("**************** before extractRawData2UserInfo() ")
    extractRawData2UserInfo(spark, hdfsPathSrc)

    spark.stop()
  }


  /**
    * create the hive table to add lava_id column to raw table
    * @param spark           spark session
    */
  def createSourceTableInHive(spark: SparkSession): Unit = {

    // create the temporary function, add the --jars ./hive-udf.jar in the spark-submit
    spark.sql(s"create temporary function lava_uuid as 'com.lavapm.hive.udf.UDFUuid' ")

    // drop the result table firstly
    spark.sql("drop table if exists tmp.user_basic_info")

    // create the table
    spark.sql(s"create table tmp.user_basic_info stored AS parquet as " +
                    s" select lava_uuid(a.mobile) as lava_id, " +
                    s" a.mobile, a.name, a.name_en, a.idno, a.gender, cast(a.age as string) as age, a.age_range, " +
                    s" a.birthday, a.nation, a.birth_season, a.zodiac, a.nationality, a.birth_place, " +
                    s" a.mobile_location, a.mobile_duration, a.bank_no, a.driving_license, a.email, " +
                    s" a.residential_province, a.residential_city, a.residential_address, " +
                    s" a.nearest_plaza, a.nearest_plaza_distance, a.shopping_zone, a.daily_activity_circle, " +
                    s" a.profession, a.professional_title, a.working_status, a.working_years, " +
                    s" a.industry_type, a.education_status, a.highest_education, a.graduate_school, a.graduate_year, " +
                    s" a.major, a.mastered_language, a.marital_status, a.has_child, " +
                    s" a.yearly_income_range, a.income_source, a.life_stage, a.financial_situation, " +
                    s" a.offline_shopping, a.offline_entertainment, a.financial_activity, a.online_activity, " +
                    s" b.mac, md5(b.mac) as mac_enc, b.imei, md5(b.imei) as imei_enc, b.idfa, md5(b.idfa) as idfa_enc, " +
                    s" c.*, cast(d.city_level as string) as city_level, md5(a.mobile) as mobile_enc, md5(a.email) as email_enc " +
                    s" from raw.user_basic_info a " +
                    s" left outer join raw.user_id_info b on a.mobile = b.mobile " +
                    s" left outer join raw.mobile_affiliation c on substring(a.mobile, 1, 7) = c.prephonenumber " +
                    s" left outer join persona.dim_city_level d on a.residential_city = d.city_name")
  }



  /**
    * create the hbase table to content the result user_info
    */
  def createTableInHBase = {

    // prepare the namespace
    if (! HBase.isNamespaceExist("persona"))
      HBase.createNamespace("persona")

    // create the user_info
    HBase.createTable("persona", "user_info", Array("basic", "device", "id", "activity", "geo", "interest", "tob"))
  }


  /**
    * extract raw data in hive into persona:user_info in hbase
    * @param spark           spark session
    * @param hdfsPathSrc    hdfs path for the raw data in hive tmp.user_basic_info
    */
  def extractRawData2UserInfo(spark: SparkSession,
                              hdfsPathSrc: String): Unit = {

    val sc = spark.sparkContext

    val age_map = Map("18以下" -> "18-",
      "18-24" -> "19~24",
      "25-34" -> "25~29",
      "35-44" -> "35~39",
      "45-54" -> "40~49",
      "55-64" -> "50+",
      "65以上" -> "50+")

    sc.broadcast(age_map)

    try{
      // read from the hdfs and process each column
      val rdd = spark.read.parquet(hdfsPathSrc).rdd.map { case row =>

        val lava_id = row.getAs[String]("lava_id").trim
        val put = new Put(Bytes.toBytes(lava_id))

        // check the mobile
        if (row.getAs[String]("mobile") != null && Mobile.validMobile(row.getAs[String]("mobile").trim))
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mobile"), Bytes.toBytes(row.getAs[String]("mobile").trim))

        // check the name
        if (row.getAs[String]("name") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("name"), Bytes.toBytes(row.getAs[String]("name").trim))

        // check the name_en
        if (row.getAs[String]("name_en") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("name_en"), Bytes.toBytes(row.getAs[String]("name_en").trim))

        // check the gender
        if (row.getAs[String]("gender") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("gender"), Bytes.toBytes(row.getAs[String]("gender").trim))

        // check the age_range
        if (row.getAs[String]("age_range") != null){
          if(age_map.contains(row.getAs[String]("age_range").trim))
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("age_range"), Bytes.toBytes(age_map(row.getAs[String]("age_range").trim)))
        }

        // check the age
        if (row.getAs[String]("age") != null && BasicInfo.isInt(row.getAs[String]("age")) ) {
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("age"), Bytes.toBytes(row.getAs[String]("age").trim))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("age_range"), Bytes.toBytes(BasicInfo.getAgeRange(row.getAs[String]("age").trim)))
        }

        // check the birthday
        if (row.getAs[String]("birthday") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("birthday"), Bytes.toBytes(row.getAs[String]("birthday").trim))

        // check the nation(民族)
        if (row.getAs[String]("nation") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("nation"), Bytes.toBytes(row.getAs[String]("nation").trim))

        // check the birth_season
        if (row.getAs[String]("birth_season") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("birth_season"), Bytes.toBytes(row.getAs[String]("birth_season").trim))

        // check the zodiac
        if (row.getAs[String]("zodiac") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("zodiac"), Bytes.toBytes(row.getAs[String]("zodiac").trim))


        // check the id_no
        if (row.getAs[String]("idno") != null && IdNo.validIdNo(row.getAs[String]("idno"))) {
          // get more info from id_no
          val (gender, age, age_range, birthday, birth_season, zodiac) = IdNo.getInfoFromIdNo(row.getAs[String]("idno"))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("id_no"), Bytes.toBytes(row.getAs[String]("idno").trim))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("gender"), Bytes.toBytes(gender))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("age"), Bytes.toBytes(age))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("age_range"), Bytes.toBytes(age_range))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("birthday"), Bytes.toBytes(birthday))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("birth_season"), Bytes.toBytes(birth_season))
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("zodiac"), Bytes.toBytes(zodiac))
        }

        // check the nationality
        if (row.getAs[String]("nationality") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("nationality"), Bytes.toBytes(row.getAs[String]("nationality").trim))

        // check the life_stage
        if (row.getAs[String]("life_stage") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("life_stage"), Bytes.toBytes(row.getAs[String]("life_stage").trim))

        // check the profession
        if (row.getAs[String]("profession") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("profession"), Bytes.toBytes(row.getAs[String]("profession").trim))

        // check the profession_title
        if (row.getAs[String]("professional_title") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("profession_title"), Bytes.toBytes(row.getAs[String]("professional_title").trim))

        // check the working_status
        if (row.getAs[String]("working_status") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_status"), Bytes.toBytes(row.getAs[String]("working_status").trim))

        // check the working_years
        //if (row.getString(29) != null)
        //  put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_years"), Bytes.toBytes(row.getString(29).trim))

        // check the industry_type
        if (row.getAs[String]("industry_type") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("industry_type"), Bytes.toBytes(row.getAs[String]("industry_type").trim))

        // check the education_status
        if (row.getAs[String]("education_status") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("education_status"), Bytes.toBytes(row.getAs[String]("education_status").trim))

        // check the highest_education
        if (row.getAs[String]("highest_education") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("highest_education"), Bytes.toBytes(row.getAs[String]("highest_education").trim))

        // check the graduate_school
        if (row.getAs[String]("graduate_school") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("graduate_school"), Bytes.toBytes(row.getAs[String]("graduate_school").trim))

        // check the graduate_year
        if (row.getAs[String]("graduate_year") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("graduate_year"), Bytes.toBytes(row.getAs[String]("graduate_year").trim))

        // check the major
        if (row.getAs[String]("major") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("major"), Bytes.toBytes(row.getAs[String]("major").trim))

        // check the foreign_language
        if (row.getAs[String]("mastered_language") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("foreign_language"), Bytes.toBytes(row.getAs[String]("mastered_language").trim))

        // check the marital_status
        if (row.getAs[String]("marital_status") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("marital_status"), Bytes.toBytes(row.getAs[String]("marital_status").trim))

        // check the has_child
        if (row.getAs[String]("has_child") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_child"), Bytes.toBytes(row.getAs[String]("has_child").trim))

        // check the yearly_income_range
        //if (row.getString(39) != null)
        //  put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("yearly_income_range"), Bytes.toBytes(row.getString(39).trim))

        // check the income_source
        if (row.getAs[String]("income_source") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("income_source"), Bytes.toBytes(row.getAs[String]("income_source").trim))

        // check the has_car
        if (row.getAs[String]("financial_situation") != null) {
          if (row.getAs[String]("financial_situation").trim.equals("无车"))
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_car"), Bytes.toBytes("0"))
          else
            put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_car"), Bytes.toBytes("1"))
        }

        // check the offline_shopping
        if (row.getAs[String]("offline_shopping") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("offline_shopping"), Bytes.toBytes(row.getAs[String]("offline_shopping").trim))

        // check the offline_entertainment
        if (row.getAs[String]("offline_entertainment") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("offline_entertainment"), Bytes.toBytes(row.getAs[String]("offline_entertainment").trim))

        // check the financial_activity
        if (row.getAs[String]("financial_activity") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("financial_activity"), Bytes.toBytes(row.getAs[String]("financial_activity").trim))

        // check the online_activity
        if (row.getAs[String]("online_activity") != null)
          put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("online_shopping"), Bytes.toBytes(row.getAs[String]("online_activity").trim))

        // set the default value
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("working_years"), Bytes.toBytes(Random.randomItem("2", "6", "9", "")))
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("yearly_income_range"), Bytes.toBytes(Random.randomItem("10000-50000", "50000-100000", "100000-200000", "")))
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("income_source"), Bytes.toBytes(Random.randomItem("工资", "经营企业", "理财投资", "")))
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("has_estate"), Bytes.toBytes(Random.randomItem("1", "0", "1", "0")))
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("consume_level"), Bytes.toBytes(Random.randomItem("H", "M", "L", "")))
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("consume_frequency"), Bytes.toBytes(Random.randomItem("H", "M", "L", "")))


        // check the mobile_operator
        if (row.getAs[String]("operator") != null)
          put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_operator"), Bytes.toBytes(row.getAs[String]("operator").trim))

        put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_language"), Bytes.toBytes(Random.randomItem("中文", "中文", "中文", "")))
        put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes(Random.randomItem("iPhone", "三星", "小米", "")))
        put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("laptop_language"), Bytes.toBytes(Random.randomItem("中文", "中文", "中文", "")))
        put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("laptop_OS"), Bytes.toBytes(Random.randomItem("Mac", "Windows 10", "", "")))
        put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("laptop_browser"), Bytes.toBytes(Random.randomItem("Chrome", "Firefox", "360", "IE")))
        put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("other_language"), Bytes.toBytes(Random.randomItem("中文", "中文", "中文", "")))
        put.addColumn(Bytes.toBytes("device"), Bytes.toBytes("other_brand"), Bytes.toBytes(Random.randomItem("iPad", "三星", "", "")))


        // check the bank_no
        if (row.getAs[String]("bank_no") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("bank_no"), Bytes.toBytes(row.getAs[String]("bank_no").trim))

        // check the driving_license
        if (row.getAs[String]("driving_license") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("driving_license"), Bytes.toBytes(row.getAs[String]("driving_license").trim))

        // check the email
        if (row.getAs[String]("email") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("email"), Bytes.toBytes(row.getAs[String]("email").trim))

        // check the email_enc
        if (row.getAs[String]("email_enc") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("email_enc"), Bytes.toBytes(row.getAs[String]("email_enc").trim))

        // check the mobile_enc
        if (row.getAs[String]("mobile_enc") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mobile_enc"), Bytes.toBytes(row.getAs[String]("mobile_enc").trim))

        // check the mac
        if (row.getAs[String]("mac") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac"), Bytes.toBytes(row.getAs[String]("mac").trim))

        // check the mac_enc
        if (row.getAs[String]("mac_enc") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac_enc"), Bytes.toBytes(row.getAs[String]("mac_enc").trim))

        // check the imei
        if (row.getAs[String]("imei") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("imei"), Bytes.toBytes(row.getAs[String]("imei").trim))

        // check the imei_enc
        if (row.getAs[String]("imei_enc") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("imei_enc"), Bytes.toBytes(row.getAs[String]("imei_enc").trim))

        // check the idfa
        if (row.getAs[String]("idfa") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("idfa"), Bytes.toBytes(row.getAs[String]("idfa").trim))

        // check the idfa_enc
        if (row.getAs[String]("idfa_enc") != null)
          put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("idfa_enc"), Bytes.toBytes(row.getAs[String]("idfa_enc").trim))


        // check the mobile_duration
        if (row.getAs[String]("mobile_duration") != null && (! row.getAs[String]("mobile_duration").trim.equals(""))) {
          put.addColumn(Bytes.toBytes("activity"), Bytes.toBytes("mobile_duration"), Bytes.toBytes(row.getAs[String]("mobile_duration").trim))
          put.addColumn(Bytes.toBytes("activity"), Bytes.toBytes("mobile_status"), Bytes.toBytes("有效"))
        }

        // check the birth_place
        if (row.getAs[String]("birth_place") != null){
          val (province, city) = Place.getProvinceAndCityFromAddress(row.getAs[String]("birth_place").trim)

          if(province != null & (! province.toString.equals("")))
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("birth_province"), Bytes.toBytes(province))

          if(city != null & (! city.toString.equals("")))
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("birth_city"), Bytes.toBytes(city))
        }

        // check the mobile_province
        if (row.getAs[String]("provience") != null)
          put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("mobile_province"), Bytes.toBytes(row.getAs[String]("provience").trim))

        // check the mobile_city
        if (row.getAs[String]("city") != null)
          put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("mobile_city"), Bytes.toBytes(row.getAs[String]("city").trim))

        // check the resident_province
        if (row.getAs[String]("residential_province") != null) {
          put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_country"), Bytes.toBytes("中国"))
          put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_province"), Bytes.toBytes(row.getAs[String]("residential_province").trim))
        }

        // check the resident_city
        if (row.getAs[String]("residential_city") != null)
          put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_city"), Bytes.toBytes(Place.getCityFromAddress(row.getAs[String]("residential_city").trim)))

        // check the city_level
        if (row.getAs[String]("city_level") != null && (! row.getAs[String]("city_level").equalsIgnoreCase("null")))
          put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("city_level"), Bytes.toBytes(row.getAs[String]("city_level").trim))

        // check the resident_address
        if (row.getAs[String]("residential_address") != null)
          put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_address"), Bytes.toBytes(row.getAs[String]("residential_address").trim))

        // check the landmark
        if (row.getAs[String]("nearest_plaza") != null)
          put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("landmark"), Bytes.toBytes(row.getAs[String]("nearest_plaza").trim))

        put
      }

      HBase.bulkLoad(spark,rdd)
    } catch {
      case ex: Exception => {println("*************** ex := " + ex.toString)}
    }

  }




}

