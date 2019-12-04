package com.lavapm.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.lavapm.utils.{HBase, Hive, UUID}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * DSP data streaming
  * Created by ryan on 30/05/2019.
  */

object DspStreaming {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("DSP Streaming")
      .enableHiveSupport()
      //set hive table dynamic import
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()

    val packageInterestMap = Hive.generateApp2InterestMap(spark)
    val locationMap = Hive.generateCity2Province(spark)
    val cityLevelMap = Hive.generateCityName2CityLevel(spark)

    val sc = spark.sparkContext

    sc.broadcast(packageInterestMap)
    sc.broadcast(locationMap)
    sc.broadcast(cityLevelMap)

    val ssc = new StreamingContext(sc, Seconds(5))

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val askSchema = createAskSchema()
    val bidSchema = createBidSchema()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.1.65:9092,192.168.1.66:9092,192.168.1.67:9092", // kafka cluster
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "testGroup",
      "auto.offset.reset" -> "latest", // 1.earliest 每次都是从头开始消费（from-beginning）2.latest 消费最新消息 3.smallest 从最早的消息开始读取
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array("ask_topic","bid_topic") //topics，可配置多个

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    if (stream != null){
      stream.foreachRDD { kafkaRDD =>
        if (kafkaRDD != null && !kafkaRDD.isEmpty()) {
          try {
            val date = new Date()

            val cacheRDD = kafkaRDD
              .map(_.value())
              .filter(_.trim != "")
              .cache()

            println("*********cacheRDD*********"+cacheRDD.count().toString())

            val askRDD = cacheRDD
              //filter to get ask log
              .filter(x => {
                try{
                  !JSON.parseObject(x).containsKey("ev")
                } catch {
                  case ex:Exception => println("*****json parsing error:" + ex.toString)
                    false
                }
              })
              .map(getRowFromAskJsonStr(_, dateFormat,date))

            println("*********askRDD*********"+askRDD.count().toString())

            val bidRDD = cacheRDD
              //filter to get bid log
              .filter(x => {
                try{
                  JSON.parseObject(x).containsKey("ev")
                } catch {
                  case ex:Exception => println("*****json parsing error:" + ex.toString)
                    false
                }
              })
              .map(getRowFromBidJsonStr(_, dateFormat,date))

            println("*********bidRDD*********"+bidRDD.count().toString())

            val askDF = spark
              .createDataFrame(askRDD, askSchema)
              //.cache()

            //load ask data to Hive
            askDF
              .coalesce(1)
              .write
              .insertInto("fact_dsp.fact_dsp_ask")

            println("*****************Hive ask saved****************")


            val bidDF = spark
              .createDataFrame(bidRDD, bidSchema)
              .cache()

            //load bid data to Hive
            bidDF
              .filter("ev = 'bid'")
              .coalesce(1)
              .write
              .insertInto("fact_dsp.fact_dsp_bid")

            bidDF
              .filter("ev = 'imp' and mid is not null")
              .coalesce(1)
              .write
              .insertInto("fact_dsp.fact_dsp_imp")

            bidDF
              .filter("ev = 'clk'")
              .coalesce(1)
              .write
              .insertInto("fact_dsp.fact_dsp_clk")

            println("*****************Hive bid saved****************")

            //load ask data to HBase
            //write2HBase(askDF,packageInterestMap,locationMap,cityLevelMap)
            //println("*****************HBase saved*******************")

          } catch {
            case ex:Exception => println("************** ex:= "+ex.toString)
          }

        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * get fields from ask log json string and return a ask message row
    * @param jsonStr      ask log json string
    * @param dateFormat   for partition in Hive fact_dsp.db
    */
  def getRowFromAskJsonStr(jsonStr:String,dateFormat:SimpleDateFormat,date: Date)= {

    // 根据ask日志数据的特征，取值
      val jsonObject = JSON.parseObject(jsonStr)
      val id = jsonObject.getString("id")
      val tm = jsonObject.getString("tm")
      val nid = jsonObject.getString("nid")
      val bid = jsonObject.getString("bid")
      val osid = jsonObject.getString("osid")
      val ubid = jsonObject.getString("ubid")
      val adxid = jsonObject.getString("adxid")
      val tobid = jsonObject.getString("tobid")
      val do_bid = jsonObject.getString("do_bid")
      val user_id = jsonObject.getString("user_id")
      val ex_name = jsonObject.getString("ex_name")
      val region = jsonObject.getString("region")
      val version = jsonObject.getString("version")
      val is_https = jsonObject.getString("is_https")
      val is_order = jsonObject.getString("is_order")
      val is_mobile = jsonObject.getString("is_mobile")
      val is_native = jsonObject.getString("is_native")
      val native = jsonObject.getString("native")
      val is_video = jsonObject.getString("is_video")
      var min_duration,max_duration:Any = null
      if (jsonObject.getJSONObject("video") != null){
        min_duration = jsonObject.getJSONObject("video").getString("min_duration")
        max_duration = jsonObject.getJSONObject("video").getString("max_duration")
      }
      val user_dict = jsonObject.getString("user_dict")
      val ip = JSON.parseObject(user_dict).getString("ip")
      val language = JSON.parseObject(user_dict).getString("language")
      val user_agent = JSON.parseObject(user_dict).getString("user_agent")
      val ad_slot = jsonObject.getString("ad_slot")
      val ad_size = JSON.parseArray(ad_slot).getJSONObject(0).getString("size")
      val ad_tagid = JSON.parseArray(ad_slot).getJSONObject(0).getString("tagid")
      val ad_min_cpm_price = JSON.parseArray(ad_slot).getJSONObject(0).getString("min_cpm_price")
      val mobile = jsonObject.getString("mobile")

      var mobile_geo,mobile_app,mobile_app_id,mobile_app_bundle,mobile_os,mobile_oss,mobile_mac,mobile_imei,mobile_idfa,
      mobile_model,mobile_brand,mobile_osver,mobile_devid,mobile_geo_type,mobile_geo_lon,mobile_geo_lat,mobile_geo_province,
      mobile_geo_city,mobile_geo_district,mobile_geo_street,mobile_app_category,mobile_app_publisher_id,mobile_app_interaction_type,
      mobile_width,mobile_height,mobile_screen_density,mobile_imsi,mobile_net,mobile_network,mobile_anid :Any = null

      if (mobile != null) {
        mobile_geo = JSON.parseObject(mobile).getString("geo")
        mobile_app = JSON.parseObject(mobile).getString("app")
        if (mobile_app != null) {
          mobile_app_id = JSON.parseObject(mobile).getJSONObject("app").getString("id")
          mobile_app_bundle = JSON.parseObject(mobile).getJSONObject("app").getString("bundle")

          mobile_app_publisher_id = JSON.parseObject(mobile).getJSONObject("app").getString("app_publisher_id")
          mobile_app_interaction_type = JSON.parseObject(mobile).getJSONObject("app").getString("app_interaction_type")
        }

        if (JSON.parseObject(JSON.parseObject(mobile).getString("device")) != null) {
          mobile_os = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("os")
          mobile_oss = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("oss")
          mobile_mac = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("mac")
          mobile_imei = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("imei")
          mobile_idfa = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("idfa")
          mobile_model = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("model")
          mobile_brand = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("brand")
          mobile_osver = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("osver")
          mobile_devid = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("devid")

          mobile_width = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("width")
          mobile_height = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("height")
          mobile_screen_density = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("screen_density")
          mobile_imsi = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("imsi")
          mobile_net = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("net")
          mobile_network = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("network")
          mobile_anid = JSON.parseObject(JSON.parseObject(mobile).getString("device")).getString("anid")
        }

        if (mobile_geo != null) {
          mobile_geo_type = JSON.parseObject(mobile).getJSONObject("geo").getString("type")
          mobile_geo_lon = JSON.parseObject(mobile).getJSONObject("geo").getString("lon")
          mobile_geo_lat = JSON.parseObject(mobile).getJSONObject("geo").getString("lat")
          mobile_geo_province = JSON.parseObject(mobile).getJSONObject("geo").getString("province")
          mobile_geo_city = JSON.parseObject(mobile).getJSONObject("geo").getString("city")
          mobile_geo_district = JSON.parseObject(mobile).getJSONObject("geo").getString("district")
          mobile_geo_street = JSON.parseObject(mobile).getJSONObject("geo").getString("street")
          mobile_app_category = JSON.parseObject(mobile).getJSONObject("geo").getString("app_category")
        }

      }

      val url_dict = jsonObject.getString("url_dict")
      var url,domain,keywords,url_title,url_referer,site_category,site_quality,page_type,page_quality,page_vertical:Any = null
      if (url_dict != null) {
        url = JSON.parseObject(url_dict).getString("url")
        domain = JSON.parseObject(url_dict).getString("domain")
        keywords = JSON.parseObject(url_dict).getString("keywords")
        url_title = JSON.parseObject(url_dict).getString("keyword")

        url_referer = JSON.parseObject(url_dict).getString("referer")
        site_category = JSON.parseObject(url_dict).getString("site_category")
        site_quality = JSON.parseObject(url_dict).getString("site_quality")
        page_type = JSON.parseObject(url_dict).getString("page_type")
        page_quality = JSON.parseObject(url_dict).getString("page_quality")
        page_vertical = JSON.parseObject(url_dict).getString("page_vertical")
      }

      val deny_dict = jsonObject.getString("deny_dict")
      val allow_dict = jsonObject.getString("allow_dict")
      val user_category = jsonObject.getString("user_category")

      var excluded_product_category:Any = null
      if (deny_dict != null) {
        excluded_product_category = JSON.parseObject(deny_dict).getString("ad_category")
      }

      var ad_pid,ad_sequence_id,ad_view_type,ad_width,ad_height,ad_location,ad_link_unit_info,ad_deal_id,ad_pmp_info,ad_allow_auction,
      ad_preferred_order_info,ad_guaranteed_orders,ad_expand_creative_info,adslot_level,ad_native_image_num,ad_allowed_non_nativead,
      ad_style_info,native_required_fields,allow_dict_creative_type,allow_dict_ad_type,deny_dict_landing_page,deny_dict_setting:Any = null

      if (JSON.parseArray(ad_slot).getJSONObject(0) != null) {
        ad_pid = JSON.parseArray(ad_slot).getJSONObject(0).getString("pid")
        ad_sequence_id = JSON.parseArray(ad_slot).getJSONObject(0).getString("sequence_id")
        ad_view_type = JSON.parseArray(ad_slot).getJSONObject(0).getString("view_type")
        ad_width = JSON.parseArray(ad_slot).getJSONObject(0).getString("width")
        ad_height = JSON.parseArray(ad_slot).getJSONObject(0).getString("height")
        ad_location = JSON.parseArray(ad_slot).getJSONObject(0).getString("ad_location")
        ad_link_unit_info = JSON.parseArray(ad_slot).getJSONObject(0).getString("link_unit_info")
        ad_deal_id = JSON.parseArray(ad_slot).getJSONObject(0).getString("deal_id")
        ad_pmp_info = JSON.parseArray(ad_slot).getJSONObject(0).getString("pmp_info")
        ad_allow_auction = JSON.parseArray(ad_slot).getJSONObject(0).getString("allow_auction")
        ad_preferred_order_info = JSON.parseArray(ad_slot).getJSONObject(0).getString("preferred_order_info")
        ad_guaranteed_orders = JSON.parseArray(ad_slot).getJSONObject(0).getString("guaranteed_orders")
        ad_expand_creative_info = JSON.parseArray(ad_slot).getJSONObject(0).getString("expand_creative_info")
        adslot_level = JSON.parseArray(ad_slot).getJSONObject(0).getString("adslot_level")
        ad_native_image_num = JSON.parseArray(ad_slot).getJSONObject(0).getString("native_image_num")
        ad_allowed_non_nativead = JSON.parseArray(ad_slot).getJSONObject(0).getString("allowed_non_nativead")
        ad_style_info = JSON.parseArray(ad_slot).getJSONObject(0).getString("style_info")

        if (JSON.parseArray(ad_slot).getJSONObject(0).getJSONObject("native") != null)
          native_required_fields = JSON.parseArray(ad_slot).getJSONObject(0).getJSONObject("native").getString("required_fields")
        if (JSON.parseArray(ad_slot).getJSONObject(0).getJSONObject("allow_dict") != null)
          allow_dict_creative_type = JSON.parseArray(ad_slot).getJSONObject(0).getJSONObject("allow_dict").getString("creative_type")
        if (JSON.parseArray(ad_slot).getJSONObject(0).getJSONObject("allow_dict") != null)
          allow_dict_ad_type = JSON.parseArray(ad_slot).getJSONObject(0).getJSONObject("allow_dict").getString("ad_type")
        if (JSON.parseArray(ad_slot).getJSONObject(0).getJSONObject("deny_dict") != null)
          deny_dict_landing_page = JSON.parseArray(ad_slot).getJSONObject(0).getJSONObject("deny_dict").getString("landing_page")
        if (JSON.parseArray(ad_slot).getJSONObject(0).getJSONObject("deny_dict") != null)
          deny_dict_setting = JSON.parseArray(ad_slot).getJSONObject(0).getJSONObject("deny_dict").getString("setting")

      }

      val time = dateFormat.format(date)
      val dt_y = time.substring(0,4)
      val dt_m = time.substring(5,7)
      val dt_d = time.substring(8,10)
      val dt_h = time.substring(11,13)
      /*val dt_y = "2019"
      val dt_m = "06"
      val dt_d = "27"
      val dt_h = "16"*/

      Row(id, tm, nid, bid, osid, ubid, adxid, tobid, do_bid, user_id, ex_name, region, version, is_https, is_order, is_mobile,
        is_native, native,is_video, min_duration, max_duration, user_dict, ip, language, user_agent, ad_slot, ad_size, ad_tagid,
        ad_min_cpm_price, mobile, mobile_geo, mobile_app, mobile_app_id, mobile_app_bundle, mobile_os, mobile_oss, mobile_mac,
        mobile_imei, mobile_idfa, mobile_model, mobile_brand, mobile_osver, mobile_devid, url_dict, url, domain, keywords,
        url_title, deny_dict, allow_dict, user_category, mobile_geo_type, mobile_geo_lon, mobile_geo_lat, mobile_geo_province,
        mobile_geo_city, mobile_geo_district, mobile_geo_street, mobile_app_category, mobile_app_publisher_id, mobile_app_interaction_type,
        url_referer, site_category, site_quality, page_type, page_quality, page_vertical, excluded_product_category, mobile_width,
        mobile_height, mobile_screen_density, mobile_imsi, mobile_net, mobile_network, mobile_anid, ad_pid, ad_sequence_id, ad_view_type,
        ad_width, ad_height, ad_location, ad_link_unit_info, ad_deal_id, ad_pmp_info, ad_allow_auction, ad_preferred_order_info,
        ad_guaranteed_orders, ad_expand_creative_info, adslot_level, ad_native_image_num, ad_allowed_non_nativead, ad_style_info,
        native_required_fields, allow_dict_creative_type, allow_dict_ad_type, deny_dict_landing_page, deny_dict_setting, dt_y, dt_m, dt_d, dt_h)

    /*ret = id+"-"+tm+"-"+nid+"-"+bid+"-"+osid+"-"+ubid+"-"+adxid+"-"+tobid+"-"+do_bid+"-"+user_id+"-"+ex_name+"-"+region+"-"+version+"-"+is_https+"-"+is_order+"-"+is_mobile+"-"+
      is_native+"-"+native+"-"+is_video+"-"+min_duration+"-"+max_duration+"-"+user_dict+"-"+ip+"-"+language+"-"+user_agent+"-"+ad_slot+"-"+ad_size+"-"+ad_tagid+"-"+
      ad_min_cpm_price+"-"+mobile+"-"+mobile_geo+"-"+mobile_app+"-"+mobile_app_id+"-"+mobile_app_bundle+"-"+mobile_os+"-"+mobile_oss+"-"+mobile_mac+"-"+
      mobile_imei+"-"+mobile_idfa+"-"+mobile_model+"-"+mobile_brand+"-"+mobile_osver+"-"+mobile_devid+"-"+url_dict+"-"+url+"-"+domain+"-"+keywords+"-"+
      url_title+"-"+deny_dict+"-"+allow_dict+"-"+user_category+"-"+mobile_geo_type+"-"+mobile_geo_lon+"-"+mobile_geo_lat+"-"+mobile_geo_province+"-"+
      mobile_geo_city+"-"+mobile_geo_district+"-"+mobile_geo_street+"-"+mobile_app_category+"-"+mobile_app_publisher_id+"-"+mobile_app_interaction_type+"-"+
      url_referer+"-"+site_category+"-"+site_quality+"-"+page_type+"-"+page_quality+"-"+page_vertical+"-"+excluded_product_category+"-"+mobile_width+"-"+
      mobile_height+"-"+mobile_screen_density+"-"+mobile_imsi+"-"+mobile_net+"-"+mobile_network+"-"+mobile_anid+"-"+ad_pid+"-"+ad_sequence_id+"-"+ad_view_type+"-"+
      ad_width+"-"+ad_height+"-"+ad_location+"-"+ad_link_unit_info+"-"+ad_deal_id+"-"+ad_pmp_info+"-"+ad_allow_auction+"-"+ad_preferred_order_info+"-"+
      ad_guaranteed_orders+"-"+ad_expand_creative_info+"-"+adslot_level+"-"+ad_native_image_num+"-"+ad_allowed_non_nativead+"-"+ad_style_info+"-"+
      native_required_fields+"-"+allow_dict_creative_type+"-"+allow_dict_ad_type+"-"+deny_dict_landing_page+"-"+deny_dict_setting+"-"+dt_y+"-"+dt_m+"-"+dt_d+"-"+dt_h
    ret*/
  }

  /**
    * get fields from bid log json string and return a bid message row
    * @param jsonStr      bid log json string
    * @param dateFormat   for partition in Hive fact_dsp.db
    */
  def getRowFromBidJsonStr(jsonStr:String,dateFormat:SimpleDateFormat,date: Date)= {

    // 根据bid日志数据的特征，取值
    val jsonObject = JSON.parseObject(jsonStr)
    val mid = jsonObject.getString("mid")
    val pid = jsonObject.getString("pid")
    val oid = jsonObject.getString("oid")
    val wd = jsonObject.getString("wd")
    val ccp = jsonObject.getString("ccp")
    val bid = jsonObject.getString("bid")
    val url = jsonObject.getString("url")
    val lg = jsonObject.getString("lg")
    val me = jsonObject.getString("me")
    val st = jsonObject.getString("st")
    val v = jsonObject.getString("v")
    val sz = jsonObject.getString("sz")
    val refer = jsonObject.getString("refer")
    val is_mobile = jsonObject.getString("is_mobile")
    val ua = jsonObject.getString("ua")
    val et = jsonObject.getString("et")
    val et_bid = jsonObject.getJSONObject("et").getString("bid")
    val et_imp = jsonObject.getJSONObject("et").getString("imp")
    val ev = jsonObject.getString("ev")
    val ip = jsonObject.getString("ip")
    val ex = jsonObject.getString("ex")
    val ot = jsonObject.getString("ot")
    val exid = jsonObject.getString("exid")
    val occ = jsonObject.getString("occ")
    val id = jsonObject.getString("id")
    val ins = jsonObject.getString("ins")
    val adid = jsonObject.getString("adid")
    val mobile = jsonObject.getString("mobile")
    var mobile_os, mobile_pai, mobile_md, mobile_id, mobile_lat, mobile_lon, mobile_w, mobile_h,
    mobile_yid, mobile_nm, mobile_ii, mobile_net, mobile_imei, mobile_mac, mobile_anid, mobile_idfa:Any = null
    if (mobile != null) {
      mobile_os = jsonObject.getJSONObject("mobile").getString("os")
      mobile_pai = jsonObject.getJSONObject("mobile").getString("pai")
      mobile_md = jsonObject.getJSONObject("mobile").getString("md")
      mobile_id = jsonObject.getJSONObject("mobile").getString("id")
      mobile_lat = jsonObject.getJSONObject("mobile").getString("lat")
      mobile_lon = jsonObject.getJSONObject("mobile").getString("lon")
      mobile_w = jsonObject.getJSONObject("mobile").getString("w")
      mobile_h = jsonObject.getJSONObject("mobile").getString("h")
      mobile_yid = jsonObject.getJSONObject("mobile").getString("yid")
      mobile_nm = jsonObject.getJSONObject("mobile").getString("nm")
      mobile_ii = jsonObject.getJSONObject("mobile").getString("ii")
      mobile_net = jsonObject.getJSONObject("mobile").getString("net")
      mobile_imei = jsonObject.getJSONObject("mobile").getString("imei")
      mobile_mac = jsonObject.getJSONObject("mobile").getString("mac")
      mobile_anid = jsonObject.getJSONObject("mobile").getString("anid")
      mobile_idfa = jsonObject.getJSONObject("mobile").getString("idfa")
    }
    val nwp = jsonObject.getString("nwp")
    val wp = jsonObject.getString("wp")
    val bp = jsonObject.getString("bp")
    val rg = jsonObject.getString("rg")
    val op = jsonObject.getString("op")
    val tt = jsonObject.getString("tt")

    val time = dateFormat.format(date)
    val dt_y = time.substring(0,4)
    val dt_m = time.substring(5,7)
    val dt_d = time.substring(8,10)
    val dt_h = time.substring(11,13)
    /*val dt_y = "2019"
    val dt_m = "06"
    val dt_d = "27"
    val dt_h = "16"*/

    Row(mid, pid, oid, wd, ccp, bid, url, lg, me, st, v, sz, refer, is_mobile, ua, et, et_bid, et_imp, ev, ip, ex, ot, exid,
        occ, id, ins, adid, mobile, mobile_os, mobile_pai, mobile_md, mobile_id, mobile_lat, mobile_lon, mobile_w, mobile_h,
        mobile_yid, mobile_nm, mobile_ii, mobile_net, mobile_imei, mobile_mac, mobile_anid, mobile_idfa, nwp, wp, bp, rg, op,
        tt, dt_y, dt_m, dt_d, dt_h)

    /*val ret = mid+"-"+pid+"-"+oid+"-"+wd+"-"+ccp+"-"+bid+"-"+url+"-"+lg+"-"+me+"-"+st+"-"+v+"-"+sz+"-"+refer+"-"+is_mobile+"-"+
              ua+"-"+et+"-"+et_bid+"-"+et_imp+"-"+ev+"-"+ip+"-"+ex+"-"+ot+"-"+exid+"-"+occ+"-"+id+"-"+ins+"-"+adid+"-"+mobile+"-"+
              mobile_os+"-"+mobile_pai+"-"+mobile_md+"-"+mobile_id+"-"+mobile_lat+"-"+mobile_lon+"-"+mobile_w+"-"+mobile_h+"-"+
              mobile_yid+"-"+mobile_nm+"-"+mobile_ii+"-"+mobile_net+"-"+mobile_imei+"-"+mobile_mac+"-"+mobile_anid+"-"+mobile_idfa+"-"+
              nwp+"-"+wp+"-"+bp+"-"+rg+"-"+op+"-"+tt+"-"+dt_y+"-"+dt_m+"-"+dt_d+"-"+dt_h

    ret*/
  }

  /**
    * create schema for ask data
    */
  def createAskSchema() = {

    val schemaString = "id,tm,nid,bid,osid,ubid,adxid,tobid,do_bid,user_id,ex_name,region,version,is_https,is_order,is_mobile," +
                      "is_native,native,is_video,min_duration,max_duration,user_dict,ip,language,user_agent,ad_slot,ad_size,ad_tagid," +
                      "ad_min_cpm_price,mobile,mobile_geo,mobile_app,mobile_app_id,mobile_app_bundle,mobile_os,mobile_oss,mobile_mac," +
                      "mobile_imei,mobile_idfa,mobile_model,mobile_brand,mobile_osver,mobile_devid,url_dict,url,domain,keywords," +
                      "url_title,deny_dict,allow_dict,user_category,mobile_geo_type,mobile_geo_lon,mobile_geo_lat,mobile_geo_province," +
                      "mobile_geo_city,mobile_geo_district,mobile_geo_street,mobile_app_category,mobile_app_publisher_id,mobile_app_interaction_type," +
                      "url_referer,site_category,site_quality,page_type,page_quality,page_vertical,excluded_product_category,mobile_width," +
                      "mobile_height,mobile_screen_density,mobile_imsi,mobile_net,mobile_network,mobile_anid,ad_pid,ad_sequence_id,ad_view_type," +
                      "ad_width,ad_height,ad_location,ad_link_unit_info,ad_deal_id,ad_pmp_info,ad_allow_auction,ad_preferred_order_info," +
                      "ad_guaranteed_orders,ad_expand_creative_info,adslot_level,ad_native_image_num,ad_allowed_non_nativead,ad_style_info," +
                      "native_required_fields,allow_dict_creative_type,allow_dict_ad_type,deny_dict_landing_page,deny_dict_setting,dt_y,dt_m,dt_d,dt_h"

    val fields = schemaString.split(",").map(StructField(_,StringType,true))

    StructType(fields)

  }

  /**
    * create schema for bid data
    */
  def createBidSchema() = {

    val schemaString = "mid,pid,oid,wd,ccp,bid,url,lg,me,st,v,sz,refer,is_mobile,ua,et,et_bid,et_imp,ev,ip,ex,ot,exid,occ,id,ins,adid," +
                      "mobile,mobile_os,mobile_pai,mobile_md,mobile_id,mobile_lat,mobile_lon,mobile_w,mobile_h,mobile_yid,mobile_nm," +
                      "mobile_ii,mobile_net,mobile_imei,mobile_mac,mobile_anid,mobile_idfa,nwp,wp,bp,rg,op,tt,dt_y,dt_m,dt_d,dt_h"

    val fields = schemaString.split(",").map(StructField(_,StringType,true))

    StructType(fields)

  }


  /**
    * write ask data to HBase table persona:user_info
    * @param askDF              ask data frame
    * @param packageInterestMap package name to interest map
    * @param locationMap        city to province map
    * @param cityLevelMap       city to city level map
    */
  def write2HBase(askDF:DataFrame,
                  packageInterestMap:Map[String,String],
                  locationMap:Map[String,String],
                  cityLevelMap:Map[String,String]) = {

    askDF.rdd.foreachPartition(partition => {

      val connection = HBase.getConnection()

      val userTableName = TableName.valueOf("persona:user_info")
      val userTable = connection.getTable(userTableName)
      val imeiTableName = TableName.valueOf("persona:imei")
      val imeiTable = connection.getTable(imeiTableName)
      val idfaTableName = TableName.valueOf("persona:idfa")
      val idfaTable = connection.getTable(idfaTableName)
      val imeiEncTableName = TableName.valueOf("persona:imei_enc")
      val imeiEncTable = connection.getTable(imeiEncTableName)
      val idfaEncTableName = TableName.valueOf("persona:idfa_enc")
      val idfaEncTable = connection.getTable(idfaEncTableName)

      partition.foreach(row => {
        try {
          if (row.getAs[String]("mobile_imei") != null && row.getAs[String]("mobile_imei").trim.length == 15 ) {
            println("********************enter into imei**")
            var userInfoPut = new Put(Bytes.toBytes("123456789"))
            val get = new Get(Bytes.toBytes(row.getAs[String]("mobile_imei").trim))
            var lava_id = ""
            if (imeiTable.exists(get)) {
              lava_id = HBase.selectTableByKey(connection,"persona","imei",row.getAs[String]("mobile_imei").trim,"id","lava_id")
            }

            if (lava_id.trim != ""){

              userInfoPut = new Put(Bytes.toBytes(lava_id))

              if (row.getAs[String]("mobile_app_bundle") != null && packageInterestMap.contains(row.getAs[String]("mobile_app_bundle").trim)){

                val interest = packageInterestMap(row.getAs[String]("mobile_app_bundle").trim)
                var userInterest = HBase.selectTableByKey(connection,"persona","user_info",lava_id,"interest","interest_tag")

                if (userInterest != null && userInterest != "") {

                  if(interest.indexOf("-") > 0)
                    userInterest = interest.split("-")(0) + "," + interest + "," + userInterest
                  else
                    userInterest = interest + "," + userInterest

                } else {

                  if(interest.indexOf("-") > 0)
                    userInterest = interest.split("-")(0) + "," + interest
                  else
                    userInterest = interest
                }

                userInfoPut.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(userInterest))
              }

            } else {
              lava_id = UUID.generateUUID()

              val userDevicePut = new Put(Bytes.toBytes(row.getAs[String]("mobile_imei").trim))
              userDevicePut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("lava_id"), Bytes.toBytes(lava_id))
              imeiTable.put(userDevicePut)

              userInfoPut = new Put(Bytes.toBytes(lava_id))

              if (row.getAs[String]("mobile_app_bundle") != null && packageInterestMap.contains(row.getAs[String]("mobile_app_bundle").trim)) {

                var interest = packageInterestMap(row.getAs[String]("mobile_app_bundle").trim)

                if (interest.indexOf("-") > 0)
                  interest = interest.split("-")(0) + "," + interest

                userInfoPut.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(interest))
              }

            }

            userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("imei"), Bytes.toBytes(row.getAs[String]("mobile_imei").trim))

            val mobile_brand = row.getAs[String]("mobile_brand")
            if (mobile_brand != null && mobile_brand.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes(mobile_brand.trim))

            val language = row.getAs[String]("language")
            if (language != null && language.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_language"), Bytes.toBytes(language.trim))

            val city = row.getAs[String]("mobile_geo_city")
            if (city != null && city.trim != "") {
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_city"), Bytes.toBytes(city.trim))

              if (locationMap.contains(city.trim))
                userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_province"), Bytes.toBytes(locationMap(city.trim)))

              if (cityLevelMap.contains(city.trim))
                userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("city_level"), Bytes.toBytes(cityLevelMap(city.trim)))

            }

            val district = row.getAs[String]("mobile_geo_district")
            if (district != null && district.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_district"), Bytes.toBytes(district.trim))

            val ip = row.getAs[String]("ip")
            if (ip != null && ip.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("ip_address"), Bytes.toBytes(ip.trim))

            val mobile_mac = row.getAs[String]("mobile_mac")
            if (mobile_mac != null && mobile_mac.trim != "") {
              if (mobile_mac.length == 32)
                userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac_enc"), Bytes.toBytes(mobile_mac.trim))
              else
                userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac"), Bytes.toBytes(mobile_mac.trim))
            }
            userTable.put(userInfoPut)
          }


          if (row.getAs[String]("mobile_imei") != null && row.getAs[String]("mobile_imei").trim.length == 32){
            println("********************enter into imei_enc**")
            var userInfoPut = new Put(Bytes.toBytes("123456789"))
            val get = new Get(Bytes.toBytes(row.getAs[String]("mobile_imei").trim))
            var lava_id = ""
            if (imeiEncTable.exists(get)) {
              lava_id = HBase.selectTableByKey(connection,"persona","imei_enc",row.getAs[String]("mobile_imei").trim,"id","lava_id")
            }

            if (lava_id.trim != ""){

              userInfoPut = new Put(Bytes.toBytes(lava_id))

              if (row.getAs[String]("mobile_app_bundle") != null && packageInterestMap.contains(row.getAs[String]("mobile_app_bundle").trim)){

                val interest = packageInterestMap(row.getAs[String]("mobile_app_bundle").trim)
                var userInterest = HBase.selectTableByKey(connection,"persona","user_info",lava_id,"interest","interest_tag")

                if (userInterest != null && userInterest != "") {

                  if(interest.indexOf("-") > 0)
                    userInterest = interest.split("-")(0) + "," + interest + "," + userInterest
                  else
                    userInterest = interest + "," + userInterest

                } else {

                  if(interest.indexOf("-") > 0)
                    userInterest = interest.split("-")(0) + "," + interest
                  else
                    userInterest = interest
                }

                userInfoPut.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(userInterest))
              }

            } else {
              lava_id = UUID.generateUUID()

              val userDevicePut = new Put(Bytes.toBytes(row.getAs[String]("mobile_imei").trim))
              userDevicePut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("lava_id"), Bytes.toBytes(lava_id))
              imeiEncTable.put(userDevicePut)

              userInfoPut = new Put(Bytes.toBytes(lava_id))

              if (row.getAs[String]("mobile_app_bundle") != null && packageInterestMap.contains(row.getAs[String]("mobile_app_bundle").trim)) {

                var interest = packageInterestMap(row.getAs[String]("mobile_app_bundle").trim)

                if (interest.indexOf("-") > 0)
                  interest = interest.split("-")(0) + "," + interest

                userInfoPut.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(interest))
              }

            }

            userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("imei_enc"), Bytes.toBytes(row.getAs[String]("mobile_imei").trim))

            val mobile_brand = row.getAs[String]("mobile_brand")
            if (mobile_brand != null && mobile_brand.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes(mobile_brand.trim))

            val language = row.getAs[String]("language")
            if (language != null && language.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_language"), Bytes.toBytes(language.trim))

            val city = row.getAs[String]("mobile_geo_city")
            if (city != null && city.trim != "") {
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_city"), Bytes.toBytes(city.trim))

              if (locationMap.contains(city.trim))
                userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_province"), Bytes.toBytes(locationMap(city.trim)))

              if (cityLevelMap.contains(city.trim))
                userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("city_level"), Bytes.toBytes(cityLevelMap(city.trim)))

            }

            val district = row.getAs[String]("mobile_geo_district")
            if (district != null && district.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_district"), Bytes.toBytes(district.trim))

            val ip = row.getAs[String]("ip")
            if (ip != null && ip.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("ip_address"), Bytes.toBytes(ip.trim))

            val mobile_mac = row.getAs[String]("mobile_mac")
            if (mobile_mac != null && mobile_mac.trim != "") {
              if (mobile_mac.length == 32)
                userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac_enc"), Bytes.toBytes(mobile_mac.trim))
              else
                userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac"), Bytes.toBytes(mobile_mac.trim))
            }
            userTable.put(userInfoPut)
          }


          if (row.getAs[String]("mobile_idfa") != null && row.getAs[String]("mobile_idfa").trim.length == 36){
            println("********************enter into idfa**")
            var userInfoPut = new Put(Bytes.toBytes("123456789"))
            val get = new Get(Bytes.toBytes(row.getAs[String]("mobile_idfa").trim))
            var lava_id = ""

            if (idfaTable.exists(get)) {
              lava_id = HBase.selectTableByKey(connection,"persona","idfa",row.getAs[String]("mobile_idfa").trim,"id","lava_id")
            }

            if (lava_id.trim != ""){

              userInfoPut = new Put(Bytes.toBytes(lava_id))

              if (row.getAs[String]("mobile_app_bundle") != null && packageInterestMap.contains(row.getAs[String]("mobile_app_bundle").trim)){

                val interest = packageInterestMap(row.getAs[String]("mobile_app_bundle").trim)
                var userInterest = HBase.selectTableByKey(connection,"persona","user_info",lava_id,"interest","interest_tag")

                if (userInterest != null && userInterest != "") {

                  if(interest.indexOf("-") > 0)
                    userInterest = interest.split("-")(0) + "," + interest + "," + userInterest
                  else
                    userInterest = interest + "," + userInterest

                } else {

                  if(interest.indexOf("-") > 0)
                    userInterest = interest.split("-")(0) + "," + interest
                  else
                    userInterest = interest
                }

                userInfoPut.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(userInterest))
              }

            } else {
              lava_id = UUID.generateUUID()

              val userDevicePut = new Put(Bytes.toBytes(row.getAs[String]("mobile_idfa").trim))
              userDevicePut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("lava_id"), Bytes.toBytes(lava_id))
              idfaTable.put(userDevicePut)

              userInfoPut = new Put(Bytes.toBytes(lava_id))

              if (row.getAs[String]("mobile_app_bundle") != null && packageInterestMap.contains(row.getAs[String]("mobile_app_bundle").trim)) {

                var interest = packageInterestMap(row.getAs[String]("mobile_app_bundle").trim)

                if (interest.indexOf("-") > 0)
                  interest = interest.split("-")(0) + "," + interest

                userInfoPut.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(interest))
              }

            }

            userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("idfa"), Bytes.toBytes(row.getAs[String]("mobile_idfa").trim))

            /*val mobile_brand = row.getAs[String]("mobile_brand")
            if (mobile_brand != null && mobile_brand.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes(mobile_brand.trim))*/
            userInfoPut.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes("apple"))

            val language = row.getAs[String]("language")
            if (language != null && language.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_language"), Bytes.toBytes(language.trim))

            val city = row.getAs[String]("mobile_geo_city")
            if (city != null && city.trim != "") {
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_city"), Bytes.toBytes(city.trim))

              if (locationMap.contains(city.trim))
                userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_province"), Bytes.toBytes(locationMap(city.trim)))

              if (cityLevelMap.contains(city.trim))
                userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("city_level"), Bytes.toBytes(cityLevelMap(city.trim)))

            }

            val district = row.getAs[String]("mobile_geo_district")
            if (district != null && district.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_district"), Bytes.toBytes(district.trim))

            val ip = row.getAs[String]("ip")
            if (ip != null && ip.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("ip_address"), Bytes.toBytes(ip.trim))

            val mobile_mac = row.getAs[String]("mobile_mac")
            if (mobile_mac != null && mobile_mac.trim != "") {
              if (mobile_mac.length == 32)
                userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac_enc"), Bytes.toBytes(mobile_mac.trim))
              else
                userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac"), Bytes.toBytes(mobile_mac.trim))
            }
            userTable.put(userInfoPut)
          }


          if (row.getAs[String]("mobile_idfa") != null && row.getAs[String]("mobile_idfa").trim.length == 32){
            println("********************enter into idfa_enc**")
            var userInfoPut = new Put(Bytes.toBytes("123456789"))
            val get = new Get(Bytes.toBytes(row.getAs[String]("mobile_idfa").trim))
            var lava_id = ""

            if (idfaEncTable.exists(get)) {
              lava_id = HBase.selectTableByKey(connection,"persona","idfa_enc",row.getAs[String]("mobile_idfa").trim,"id","lava_id")
            }

            if (lava_id.trim != ""){

              userInfoPut = new Put(Bytes.toBytes(lava_id))

              if (row.getAs[String]("mobile_app_bundle") != null && packageInterestMap.contains(row.getAs[String]("mobile_app_bundle").trim)){

                val interest = packageInterestMap(row.getAs[String]("mobile_app_bundle").trim)
                var userInterest = HBase.selectTableByKey(connection,"persona","user_info",lava_id,"interest","interest_tag")

                if (userInterest != null && userInterest != "") {

                  if(interest.indexOf("-") > 0)
                    userInterest = interest.split("-")(0) + "," + interest + "," + userInterest
                  else
                    userInterest = interest + "," + userInterest

                } else {

                  if(interest.indexOf("-") > 0)
                    userInterest = interest.split("-")(0) + "," + interest
                  else
                    userInterest = interest
                }

                userInfoPut.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(userInterest))
              }

            } else {
              lava_id = UUID.generateUUID()

              val userDevicePut = new Put(Bytes.toBytes(row.getAs[String]("mobile_idfa").trim))
              userDevicePut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("lava_id"), Bytes.toBytes(lava_id))
              idfaEncTable.put(userDevicePut)

              userInfoPut = new Put(Bytes.toBytes(lava_id))

              if (row.getAs[String]("mobile_app_bundle") != null && packageInterestMap.contains(row.getAs[String]("mobile_app_bundle").trim)) {

                var interest = packageInterestMap(row.getAs[String]("mobile_app_bundle").trim)

                if (interest.indexOf("-") > 0)
                  interest = interest.split("-")(0) + "," + interest

                userInfoPut.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("interest_tag"), Bytes.toBytes(interest))
              }

            }

            userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("idfa_enc"), Bytes.toBytes(row.getAs[String]("mobile_idfa").trim))

            /*val mobile_brand = row.getAs[String]("mobile_brand")
            if (mobile_brand != null && mobile_brand.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes(mobile_brand.trim))*/
            userInfoPut.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_brand"), Bytes.toBytes("apple"))

            val language = row.getAs[String]("language")
            if (language != null && language.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("device"), Bytes.toBytes("mobile_language"), Bytes.toBytes(language.trim))

            val city = row.getAs[String]("mobile_geo_city")
            if (city != null && city.trim != "") {
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_city"), Bytes.toBytes(city.trim))

              if (locationMap.contains(city.trim))
                userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_province"), Bytes.toBytes(locationMap(city.trim)))

              if (cityLevelMap.contains(city.trim))
                userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("city_level"), Bytes.toBytes(cityLevelMap(city.trim)))

            }

            val district = row.getAs[String]("mobile_geo_district")
            if (district != null && district.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_district"), Bytes.toBytes(district.trim))

            val ip = row.getAs[String]("ip")
            if (ip != null && ip.trim != "")
              userInfoPut.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("ip_address"), Bytes.toBytes(ip.trim))

            val mobile_mac = row.getAs[String]("mobile_mac")
            if (mobile_mac != null && mobile_mac.trim != "") {
              if (mobile_mac.length == 32)
                userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac_enc"), Bytes.toBytes(mobile_mac.trim))
              else
                userInfoPut.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mac"), Bytes.toBytes(mobile_mac.trim))
            }
            userTable.put(userInfoPut)

          } else {
            println("********************No condition is entered**")
          }

        } catch {
          case ex:Exception => {println("***************hbase ex := " + ex.toString)}
        }
      })
      connection.close()
    })
  }
}

