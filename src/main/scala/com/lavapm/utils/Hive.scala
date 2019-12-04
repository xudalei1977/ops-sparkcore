package com.lavapm.utils

import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
  * Hive operation
  * Created by dalei on 6/28/2018.
  * Modified by ryan on 1/2/2019.
  */
object Hive {


  /**
    * generate a key-value map for app package name to interest from the dim_dsp.dim_app_interest table in hive
    */
  def generateApp2InterestMap(spark: SparkSession) : Map[String, String] = {

    import spark.implicits._

    val arr1 = spark.sql("select distinct package_name, interest from dim_dsp.dim_app_interest")
      .map {
        case Row(package_name: String, interest: String) => (package_name, interest)
        case _ => ("", "")
      }.collect()

    var map = Map("" -> "")
    arr1.foreach(row => map += (row._1 -> row._2))

    map
  }

  /**
    * generate a key-value map for cisco web ip to location tuple4 from the sandbox_cisco.web_his table in hive
    */
  def generateIp2LocationMap(spark: SparkSession,tableName:String) = {

    import spark.implicits._

    val arr1 = spark.sql("select distinct ip from " + tableName + "")
      .map {row => {
        val ip = row.getAs[String]("ip").trim
        val locationStr = Http.get(ip)
        val tuple4 = Json.getLocationFromJsonString(locationStr)
        (ip,tuple4)
      }}
      .collect()

    var map = Map("" -> ("","","",""))
    arr1.foreach(line => map += (line._1 -> line._2))
    map
  }

  /**
    * generate a key-value map for city name to city level from the persona.dim_city_level table in hive
    */
  def generateCityName2CityLevel(spark: SparkSession) : Map[String,String] = {

    import spark.implicits._

    val arr1 = spark.sql("select distinct city_name, city_level from persona.dim_city_level")
      .map {
        case Row(city_name: String, city_level: Int) => (city_name, city_level.toString)
        case _ => ("", "")
      }.collect()

    var map = Map("" -> "")
    arr1.foreach(row => map += (row._1 -> row._2))

    map
  }


  /**
    * generate a key-value map for district to province and city from the persona.province_city table in hive
    */
  def generateCity2Province(spark: SparkSession) : Map[String,String] = {

    import spark.implicits._

    val arr = spark.sql("select distinct province,city from persona.province_city")
      .map {
        case Row(province: String, city: String) => (province, city)
        case _ => ("", "")
      }.collect()

    var map = Map("" -> "")
    arr.foreach(row => map += (row._2 -> row._1))

    map
  }


  /**
    * generate a key-value map for interest category from the persona.dim_category table in hive
    */
  def generateInterestCategoryMap: (Map[String, String], Map[String, String]) = {

    //    val conf = new SparkConf().setAppName("Hive function")
    //    val sc = new SparkContext(conf)
    //    val hiveContext = new HiveContext(sc)
    //
    //    val arr1 = hiveContext.sql("select distinct category_name_l1, category_id_l1 from persona.dim_category order by category_id_l1")
    //                  .map {
    //                         case Row(category_name_l1: String, category_id_l1: String) => (category_name_l1, category_id_l1)
    //                         case _ => ("", "")
    //                  }.collect()
    //
    //    var map_l1 = Map("" -> "")
    //    arr1.foreach(row => map_l1 += (row._1 -> row._2))
    //
    //    val arr2 = hiveContext.sql("select distinct concat(concat(category_name_l1, '-'), category_name_l2), category_id_l2 from persona.dim_category order by category_id_l2")
    //                  .map {
    //                         case Row(category_name_l2: String, category_id_l2: String) => (category_name_l2, category_id_l2)
    //                         case _ => ("", "")
    //                  }.collect()
    //
    //    var map_l2 = Map("" -> "")
    //    arr2.foreach(row => map_l2 += (row._1 -> row._2))
    //
    //    sc.stop

    val map_l1 = Map("家居生活" -> "111", "农业" -> "121", "烟酒" -> "117", "办公" -> "118", "教育培训" -> "108", "健康医疗" -> "110",
      "数码产品" -> "101", "旅游" -> "116", "母婴育儿" -> "112", "金融" -> "104", "IT互联网" -> "102", "文化娱乐" -> "114",
      "首饰饰品" -> "106", "美容及化妆品" -> "107", "房地产" -> "119", "运动健身" -> "113", "游戏" -> "109", "汽车" -> "103",
      "美食" -> "115", "服装" -> "105", "服务" -> "122", "工业" -> "120")
    val map_l2 = Map("烟酒-洋酒" -> "117005", "教育培训-人力资源" -> "108011", "美容及化妆品-丰胸" -> "107006", "IT互联网-移动通信服务" -> "102001",
      "服装-女装" -> "105001", "运动健身-水上运动周边" -> "113011",  "教育培训-公务员" -> "108008", "汽车-豪华车" -> "103005",
      "数码产品-游戏机" -> "101008", "游戏-桌游" -> "109005", "健康医疗-疾病" -> "110005", "美食-中餐" -> "115003", "母婴育儿-胎教" -> "112002",
      "汽车-改装" -> "103012", "数码产品-电脑外设" -> "101003", "游戏-电子竞技" -> "109004", "教育培训-IT培训" -> "108006", "工业-工业原料" -> "120002",
      "农业-渔" -> "121004", "教育培训-职业教育" -> "108009", "教育培训-外语培训" -> "108012", "金融-贵金属" -> "104008", "数码产品-移动存储" -> "101007",
      "服装-牛仔裤" -> "105015", "教育培训-会计师" -> "108013", "教育培训-幼儿及学前教育" -> "108001", "金融-银行产品" -> "104001", "房地产-写字楼" -> "119004",
      "文化娱乐-传统文化" -> "114010", "健康医疗-性健康" -> "110003", "服务-咨询" -> "122003", "烟酒-香烟" -> "117006", "汽车-微型车" -> "103001",
      "数码产品-台式机及DIY" -> "101010", "美容及化妆品-香水" -> "107007", "健康医疗-儿童健康" -> "110006", "金融-债券" -> "104003", "烟酒-葡萄酒" -> "117004",
      "健康医疗-心理" -> "110002", "汽车-美容保养" -> "103019", "工业-化工" -> "120003", "IT互联网-互联网通信服务" -> "102005", "服装-婚纱礼服" -> "105020",
      "烟酒-白酒" -> "117003", "烟酒-果酒" -> "117002", "健康医疗-保健" -> "110001", "金融-期货" -> "104006", "烟酒-酒具" -> "117009",
      "美容及化妆品-美甲" -> "107005", "旅游-酒店服务" -> "116001", "汽车-特种车辆" -> "103017", "运动健身-足球运动周边" -> "113001", "金融-外汇" -> "104009",
      "汽车-中大型车" -> "103004", "数码产品-网络设备" -> "101004", "健康医疗-保健器材" -> "110008", "办公-文具" -> "118001", "办公-投影仪" -> "118006",
      "汽车-跑车" -> "103008", "美容及化妆品-彩妆" -> "107004", "汽车-二手车" -> "103014", "汽车-驾校和陪练" -> "103018", "文化娱乐-音乐" -> "114004",
      "汽车-GPS" -> "103013", "数码产品-手机" -> "101001", "汽车-MPV" -> "103007", "家居生活-日用品" -> "111006", "服务-法律" -> "122004",
      "文化娱乐-会所" -> "114009", "运动健身-减肥瘦身" -> "113016", "首饰饰品-眼镜" -> "106002", "服装-袜子" -> "105023", "IT互联网-软件" -> "102007",
      "运动健身-瑜伽周边" -> "113015", "运动健身-田径运动周边" -> "113010", "母婴育儿-怀孕" -> "112001", "服装-男鞋" -> "105005", "烟酒-啤酒" -> "117001",
      "金融-股票" -> "104002", "母婴育儿-早教" -> "112006", "数码产品-无线电器材" -> "101009", "文化娱乐-影碟光盘" -> "114013", "美容及化妆品-美体" -> "107002",
      "家居生活-厨卫用品" -> "111004", "汽车-汽车维修" -> "103010", "游戏-点卡及虚拟货币" -> "109007", "服装-女鞋" -> "105004", "家居生活-小家电" -> "111003",
      "工业-工业设备" -> "120001", "工业-制造" -> "120009", "美容及化妆品-美发" -> "107008", "办公-打印机" -> "118003", "教育培训-留学移民" -> "108010",
      "数码产品-移动影音" -> "101012", "运动健身-室内健身" -> "113018", "美食-西餐" -> "115004", "IT互联网-互联网存储服务" -> "102004", "文化娱乐-摄影" -> "114014",
      "服装-衬衫" -> "105016", "IT互联网-支付平台服务" -> "102006", "家居生活-家用电器" -> "111002", "教育培训-司法考试" -> "108007",
      "数码产品-笔记本电脑" -> "101002", "家居生活-防盗用品" -> "111010", "金融-会计审计" -> "104012", "美食-茶及茶文化" -> "115002",
      "农业-农" -> "121001", "数码产品-数码相机摄像机" -> "101006", "汽车-SUV" -> "103006", "服装-家居服" -> "105003", "服务-设计服务" -> "122002",
      "家居生活-家具" -> "111001", "汽车-货车" -> "103015", "工业-金属" -> "120005", "首饰饰品-金饰" -> "106005", "运动健身-冰上运动周边" -> "113012",
      "母婴育儿-玩具" -> "112007", "办公-会议用品" -> "118005", "文化娱乐-书籍" -> "114001", "服装-内衣" -> "105011", "游戏-Cosplay" -> "109006",
      "首饰饰品-翡翠" -> "106004", "运动健身-羽毛球运动周边" -> "113004", "数码产品-单反及摄影器材" -> "101011", "美容及化妆品-美容护肤" -> "107001",
      "教育培训-高考周边" -> "108003", "工业-环境工业" -> "120011", "母婴育儿-婴儿食品" -> "112004", "烟酒-烟具" -> "117008", "首饰饰品-珠宝" -> "106001",
      "游戏-游戏周边" -> "109011", "金融-期指" -> "104010", "服装-皮草" -> "105013", "旅游-自驾游" -> "116004", "文化娱乐-明星娱乐" -> "114005",
      "文化娱乐-演出" -> "114008", "运动健身-篮球运动周边" -> "113002", "家居生活-灯具" -> "111009", "IT互联网-服务器" -> "102003", "文化娱乐-电影电视" -> "114003",
      "服装-羊毛羊绒" -> "105021", "服装-男装" -> "105002", "运动健身-棋牌周边" -> "113014", "游戏-游戏平台" -> "109008", "服装-围巾" -> "105009",
      "旅游-机票火车票" -> "116002", "数码产品-电子词典学习机" -> "101014", "房地产-新房" -> "119001", "运动健身-高尔夫运动周边" -> "113008",
      "文化娱乐-乐器" -> "114006", "运动健身-网球运动周边" -> "113006", "金融-基金" -> "104005", "运动健身-马术运动周边" -> "113009", "农业-牧" -> "121003",
      "服务-生活服务" -> "122005", "健康医疗-医疗服务" -> "110004", "服装-童装" -> "105012", "母婴育儿-婴儿用品" -> "112005", "工业-能源" -> "120006",
      "服务-物流配送" -> "122001", "首饰饰品-钻石" -> "106003", "金融-期权" -> "104007", "旅游-国内游" -> "116006", "工业-印刷" -> "120010",
      "文化娱乐-报刊杂志" -> "114002", "美容及化妆品-精油" -> "107003", "美食-副食" -> "115007", "美食-饮料" -> "115005", "工业-工业加工" -> "120008",
      "烟酒-香槟" -> "117010", "运动健身-台球运动周边" -> "113007", "数码产品-音响HIFI" -> "101013", "汽车-汽车配件" -> "103009", "母婴育儿-营养品" -> "112003",
      "IT互联网-3G服务" -> "102002", "办公-复印机及一体机" -> "118002", "服装-泳装" -> "105022", "服装-童鞋" -> "105018", "运动健身-排球运动周边" -> "113005",
      "工业-纺织" -> "120004", "运动健身-赛车周边" -> "113013", "家居生活-宠物及宠物用品" -> "111008", "家居生活-装修" -> "111007", "教育培训-中考周边" -> "108002",
      "工业-矿业产品" -> "120007", "文化娱乐-工艺品" -> "114011", "首饰饰品-手表" -> "106007", "金融-保险" -> "104004", "游戏-游艺厅" -> "109012",
      "游戏-手机游戏" -> "109010", "数码产品-生活数码" -> "101015", "汽车-中型车" -> "103003", "游戏-网游" -> "109003", "美容及化妆品-整容" -> "107009",
      "金融-典当" -> "104011", "运动健身-乒乓球运动周边" -> "113003", "旅游-出境游" -> "116003", "数码产品-平板电脑及PDA" -> "101005", "汽车-小型车" -> "103020",
      "服装-运动服" -> "105008", "服装-休闲鞋" -> "105007", "游戏-单机游戏" -> "109001", "办公-办公家具" -> "118007", "房地产-二手房" -> "119003",
      "旅游-户外探险" -> "116005", "IT互联网-电子商务" -> "102008", "汽车-客车" -> "103016", "服装-丝袜" -> "105019", "汽车-汽车用品" -> "103011",
      "服装-运动鞋" -> "105006", "IT互联网-域名及虚拟主机服务" -> "102009", "教育培训-考研周边" -> "108004", "服装-服装面料" -> "105014",
      "家居生活-家居饰品" -> "111005", "办公-传真机" -> "118004", "运动健身-户外运动" -> "113017", "文化娱乐-彩票" -> "114012", "服装-羽绒服" -> "105017",
      "房地产-出租" -> "119002", "美食-日韩料理" -> "115006", "汽车-紧凑型车" -> "103002", "游戏-小游戏" -> "109009", "游戏-网页游戏" -> "109002",
      "教育培训-职业考试周边" -> "108005", "烟酒-酒文化" -> "117007", "服装-帽子" -> "105010", "健康医疗-食疗" -> "110007", "首饰饰品-时装箱包" -> "106006",
      "农业-林" -> "121002", "文化娱乐-古董字画" -> "114007", "美食-小吃" -> "115001")

    (map_l1, map_l2)
  }


  /**
    * retrieve user interest from HBase table persona:user_info, and keep them in hive table persona.user_interest
    * and create lava id to interest pair rdd
    * @param spark       spark session
    */
  def transferInterestFromUserInfo2UserInterestRDD(spark: SparkSession)= {

    val userInfoLavaId2InterestRDD = spark
      .sql("SELECT * FROM persona.user_interest WHERE interest IS NOT NULL").rdd
      .map(row => (row.getAs[String]("lava_id"),row.getAs[String]("interest")))

    userInfoLavaId2InterestRDD
  }


  /**
    * retrieve user mobile from HBase table persona:user_info, and keep them in hive table persona.user_mobile
    * and create mobile to lava id pair rdd
    * @param spark       spark session
    */
  def transferMobileFromUserInfo2UserMobileRDD(spark: SparkSession)= {

    val mobile2LavaIdRDD = spark
      .sql("SELECT md5(mobile) as mobile_enc,lava_id FROM persona.user_mobile WHERE mobile IS NOT NULL and length(trim(mobile)) > 0")
      .rdd
      .map(row => (row.getAs[String]("mobile_enc"),row.getAs[String]("lava_id")))

    mobile2LavaIdRDD
  }


  /**
    * retrieve user mobile_enc from HBase table persona:user_info, and keep them in hive table persona.user_mobile_enc
    * and create mobile_enc to lava id pair rdd
    * @param spark       spark session
    */
  def transferMobileEncFromUserInfo2UserMobileEncRDD(spark: SparkSession)= {

    val mobileEnc2LavaIdRDD = spark
      .sql("SELECT * FROM persona.user_mobile_enc WHERE mobile_enc IS NOT NULL")
      .rdd
      .map(row => (row.getAs[String]("mobile_enc"),row.getAs[String]("lava_id")))

    mobileEnc2LavaIdRDD
  }


  /**
    * retrieve user email from HBase table persona:user_info, and keep them in hive table persona.user_email
    * and create email to lava id pair rdd
    * @param spark   spark session
    */
  def transferEmailFromUserInfo2UserEmailRDD(spark: SparkSession)= {

    val email2LavaIdRDD = spark
      .sql("SELECT md5(email) as email_enc,lava_id FROM persona.user_email WHERE email IS NOT NULL and length(trim(email)) > 0")
      .rdd
      .map(row => (row.getAs[String]("email_enc"),row.getAs[String]("lava_id")))

    email2LavaIdRDD
  }


  /**
    * retrieve user email_enc from HBase table persona:user_info, and keep them in hive table persona.user_email_enc
    * and create email_enc to lava id pair rdd
    * @param spark       spark session
    */
  def transferEmailEncFromUserInfo2UserEmailEncRDD(spark: SparkSession)= {

    val emailEnc2LavaIdRDD = spark
      .sql("SELECT * FROM persona.user_email_enc WHERE email_enc IS NOT NULL")
      .rdd
      .map(row => (row.getAs[String]("email_enc"),row.getAs[String]("lava_id")))

    emailEnc2LavaIdRDD
  }


  /**
    * retrieve user imei from HBase table persona:user_info, and keep them in hive table persona.user_imei
    * and create imei to lava id pair rdd
    * @param spark       spark session
    */
  def transferImeiFromUserInfo2UserImeiRDD(spark: SparkSession)= {

    val imei2LavaIdRDD = spark
      .sql("SELECT * FROM persona.user_imei WHERE length(imei) = 15")
      .rdd
      .map(row => (row.getAs[String]("imei"),row.getAs[String]("lava_id")))

    imei2LavaIdRDD
  }


  /**
    * retrieve user imei_enc from HBase table persona:user_info, and keep them in hive table persona.user_imei_enc
    * and create imei_enc to lava id pair rdd
    * @param spark       spark session
    */
  def transferImeiEncFromUserInfo2UserImeiEncRDD(spark: SparkSession)= {

    val imeiEnc2LavaIdRDD = spark
      .sql("SELECT * FROM persona.user_imei_enc WHERE length(imei_enc) = 32")
      .rdd
      .map(row => (row.getAs[String]("imei_enc"),row.getAs[String]("lava_id")))

    imeiEnc2LavaIdRDD
  }


  /**
    * retrieve user idfa from HBase table persona:user_info, and keep them in hive table persona.user_idfa
    * and create idfa to lava id pair rdd
    * @param spark       spark session
    */
  def transferIdfaFromUserInfo2UserIdfaRDD(spark: SparkSession)= {

    val idfa2LavaIdRDD = spark
      .sql("SELECT * FROM persona.user_idfa WHERE length(idfa) = 36")
      .rdd
      .map(row => (row.getAs[String]("idfa"),row.getAs[String]("lava_id")))

    idfa2LavaIdRDD
  }


  /**
    * retrieve user idfa_enc from HBase table persona:user_info, and keep them in hive table persona.user_idfa_enc
    * and create idfa_enc to lava id pair rdd
    * @param spark       spark session
    */
  def transferIdfaEncFromUserInfo2UserIdfaEncRDD(spark: SparkSession)= {

    val idfaEnc2LavaIdRDD = spark
      .sql("SELECT * FROM persona.user_idfa_enc WHERE length(idfa_enc) = 32")
      .rdd
      .map(row => (row.getAs[String]("idfa_enc"),row.getAs[String]("lava_id")))

    idfaEnc2LavaIdRDD
  }


  /**
    * generate a key-value map for mobile operator
    */
  def generateMobileOperator: Map[String,String] = {

    val map = Map("46000" -> "中国移动", "46001" -> "中国联通", "46002" -> "中国移动", "46003" -> "中国电信",
      "46005" -> "中国电信", "46006" -> "中国联通", "46007" -> "中国移动","46011" -> "中国电信")

    map
  }


  /**
    * generate a schema for rdd to create data frame
    */
  def generateSchema(schemaStr:String,splitter:String) = {

    val fields = schemaStr.split(splitter).map(StructField(_,StringType,true))

    StructType(fields)
  }
}
