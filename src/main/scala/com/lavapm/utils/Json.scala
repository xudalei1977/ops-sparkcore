package com.lavapm.utils


import java.text.SimpleDateFormat

import com.google.gson._

import scala.collection.mutable.ArrayBuffer


/**
  * Json
  * Created by dalei on 8/7/18.
  * Modified by ryan on 26/12/2018.
  */
object Json {

  /**
    * get table name and filter array from the json string
    * return: relation operator
    *         table name
    *         (_1 column family  _2 column name  _3 filter value -4 compare type)
    */
  def getTableNameAndFilterFromJsonString(jsonStr: String): (String, String, Array[Tuple4[String, String, String, String]]) = {

    val parse = new JsonParser
    var operator = ""
    var tableName = ""
    val arrBuf = new ArrayBuffer[Tuple4[String, String, String, String]]

    try {
      val parser = new JsonParser
      val json = parser.parse(jsonStr).asInstanceOf[JsonObject]
      operator = json.get("relation_operator").getAsString

      val arr = json.get("filters").getAsJsonArray
      tableName = arr.get(0).getAsJsonObject.get("table_name").getAsString

      val filters = arr.get(0).getAsJsonObject.get("filter").getAsJsonArray
      for(i <- 0 to filters.size - 1) {
        val fitler = filters.get(i).getAsJsonObject
        arrBuf.append(("info", fitler.get("column_name").getAsString, fitler.get("value").getAsString, fitler.get("compare_operator").getAsString))
      }

//      {"relation_operator": "or",
//        "filters": [
//        {"table_name": "persona.user_basic_info", "filter": [{"column_name": "age", "compare_operator": "=", "type":"Int", "value": "20"}, {"column_name": "gender", "compare_operator": "=", "type":"String", "value": "F"}]},
//        {"table_name": "", "filter": []},
//        {"table_name": "", "filter": []}
//        ]
//      }

    } catch {
      case e: JsonIOException =>
        e.printStackTrace()
      case e: JsonSyntaxException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    }

    (operator, tableName, arrBuf.toArray)

  }


  /**
    * get table name and show column array from the json string
    * return: table name
    *         (_1 column family  _2 column name  _3 column type)
    */
  def getTableNameAndShowColumnFromJsonString(jsonStr: String): (String, Array[Tuple3[String, String, String]]) = {

    val parse = new JsonParser
    var tableName = ""
    val arrBuf = new ArrayBuffer[Tuple3[String, String, String]]

    try {
      val parser = new JsonParser
      val json = parser.parse(jsonStr).asInstanceOf[JsonObject]

      val arr = json.get("show_columns").getAsJsonArray
      tableName = arr.get(0).getAsJsonObject.get("table_name").getAsString

      val columns = arr.get(0).getAsJsonObject.get("show_column").getAsJsonArray
      for(i <- 0 to columns.size - 1) {
        val column = columns.get(i).getAsJsonObject
        arrBuf.append(("info", column.get("column_name").getAsString, "String"))
      }

//      {"show_columns": [
//        {"table_name": "persona.user_basic_info", "show_column": [{"column_name": "age"}, {"column_name": "gender"}]},
//        {"table_name": "", "show_column": []},
//        {"table_name": "", "show_column": []}
//        ]
//      }

    } catch {
      case e: JsonIOException =>
        e.printStackTrace()
      case e: JsonSyntaxException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    }

    (tableName, arrBuf.toArray)

  }

  /**
    * get location tuple4 (country name,province name,city name,district name) from the json string
    * @param jsonStr json string parsed from ip
    * @return location tuple4
    */
  def getLocationFromJsonString(jsonStr:String):Tuple4[String,String,String,String] = {
    var countryName = ""
    var provinceName = ""
    var cityName = ""
    var districtName = ""

    try {
      val parser = new JsonParser
      val jsonObject = parser.parse(jsonStr).asInstanceOf[JsonArray].get(0).getAsJsonObject
      val countryElement = jsonObject.get("country")
      val provinceElement = jsonObject.get("province")
      val cityElement = jsonObject.get("city")
      val districtElement = jsonObject.get("district")

      if (countryElement != null)
        countryName = countryElement.getAsString.trim
      if (provinceElement != null)
        provinceName = provinceElement.getAsString.trim
      if (cityElement != null)
        cityName = cityElement.getAsString.trim
      if (districtElement != null)
        districtName = districtElement.getAsString.trim
    } catch {
      case e: JsonIOException =>
        e.printStackTrace()
      case e: JsonSyntaxException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    }

    Tuple4(countryName,provinceName,cityName,districtName)
  }

  /**
    * get province name from the json string
    * @param jsonStr json string parsed from ip
    * @return country name
    */
  def getCountryNameFromJsonString(jsonStr:String):String = {
    var countryName = ""
    try {
      val parser = new JsonParser
      val jsonArray = parser.parse(jsonStr).asInstanceOf[JsonArray]
      val jsonElement = jsonArray.get(0).getAsJsonObject.get("country")
      if (jsonElement != null)
        countryName = jsonElement.getAsString.trim
    } catch {
      case e: JsonIOException =>
        e.printStackTrace()
      case e: JsonSyntaxException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    }

    countryName
  }

  /**
    * get province name from the json string
    * @param jsonStr json string parsed from ip
    * @return country name
    */
  def getProvinceNameFromJsonString(jsonStr:String):String = {
    var provinceName = ""
    try {
      val parser = new JsonParser
      val jsonArray = parser.parse(jsonStr).asInstanceOf[JsonArray]
      val jsonElement = jsonArray.get(0).getAsJsonObject.get("province")
      if (jsonElement != null)
        provinceName = jsonElement.getAsString.trim
    } catch {
      case e: JsonIOException =>
        e.printStackTrace()
      case e: JsonSyntaxException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    }

    provinceName
  }

  /**
    * get city name from the json string
    * @param jsonStr json string parsed from ip
    * @return country name
    */
  def getCityNameFromJsonString(jsonStr:String):String = {
    var cityName = ""
    try {
      val parser = new JsonParser
      val jsonArray = parser.parse(jsonStr).asInstanceOf[JsonArray]
      val jsonElement = jsonArray.get(0).getAsJsonObject.get("city")
      if (jsonElement != null)
        cityName = jsonElement.getAsString.trim
    } catch {
      case e: JsonIOException =>
        e.printStackTrace()
      case e: JsonSyntaxException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    }

    cityName
  }

  /**
    * get district name from the json string
    * @param jsonStr json string parsed from ip
    * @return country name
    */
  def getDistrictNameFromJsonString(jsonStr:String):String = {
    var districtName = ""
    try {
      val parser = new JsonParser
      val jsonArray = parser.parse(jsonStr).asInstanceOf[JsonArray]
      val jsonElement = jsonArray.get(0).getAsJsonObject.get("district")
      if (jsonElement != null)
        districtName = jsonElement.getAsString.trim
    } catch {
      case e: JsonIOException =>
        e.printStackTrace()
      case e: JsonSyntaxException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    }

    districtName
  }

  /**
    * get clk date from json string "et" in hive table fact_dsp.fact_dsp_clk
    * @param jsonStr json string
    */
  def getClkDateFromJsonString(jsonStr:String):String = {
    var clk_date = ""
    try {
      val parser = new JsonParser
      val jsonObject = parser.parse(jsonStr).asInstanceOf[JsonObject]
      if (jsonObject.get("clk") != null)
        clk_date = jsonObject.get("clk").getAsString
    } catch {
      case e: JsonIOException =>
        e.printStackTrace()
      case e: JsonSyntaxException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    }

    clk_date
  }

  def main(args: Array[String]): Unit = {
   val (retStr0, retStr1, retArr1) = getTableNameAndFilterFromJsonString("{\"relation_operator\": \"or\", \n\"filters\": [\n{\"table_name\": \"persona.user_basic_info\", \"filter\": [{\"column_name\": \"age\", \"compare_operator\": \"=\", \"type\":\"Int\", \"value\": \"20\"}, {\"column_name\": \"gender\", \"compare_operator\": \"=\", \"type\":\"String\", \"value\": \"F\"}]}, \n{\"table_name\": \"\", \"filter\": []},\n{\"table_name\": \"\", \"filter\": []}\n]\n}")
    println("*********************** " + retStr0)
    println("*********************** " + retStr1)
    println("*********************** " + retArr1(0).toString)

    val (retStr2, retArr2) = getTableNameAndShowColumnFromJsonString("{\"show_columns\": [\n{\"table_name\": \"persona.user_basic_info\", \"show_column\": [{\"column_name\": \"age\"}, {\"column_name\": \"gender\"}]}, \n{\"table_name\": \"\", \"show_column\": []},\n{\"table_name\": \"\", \"show_column\": []}\n]\n}")
    println("*********************** " + retStr2)
    println("*********************** " + retArr2(0).toString)

    val head = "'\"tenant_id\": \"lavapm\",\"crowd_id\": 123, \"object_code\": \"METAACCOUNT20180530001\",\"batch_no\": \"lavapm\"'"
    val show_col = Array(("info", "age_range", "String"),("info", "category_name_l1", "String"))
    var sql = "select concat('{', " + head

    for (i <- show_col) {
      sql += ", ', \"" + i._2 + "\":\"', " + i._2 + ", '\"' "
    }

    sql += ", '}') from persona.user_interest_info"
    println(sql)

    println("[abc]".replaceAll("[\\[\\]]",""))
    println("*********************** " + getCountryNameFromJsonString("[{\"continent\":\"亚洲\",\"country\":\"中国\",\"province\":\"上海市\",\"city\":\"上海市\",\"wgs_lon\":\"121.468973\",\"maxip\":3748189695,\"minip\":3748188672,\"wgs_lat\":\"31.232382\",\"id\":\"10516122\",\"_version_\":1614481601298694155}]"))
    println("*********************** " + getClkDateFromJsonString("{\"bid\":1545010040,\"clk\":1545010050,\"imp\":1545010048}"))

    val date = new SimpleDateFormat("yyyy/M/d H:m:s").parse("2017/11/4 1:1:1")
    val now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)
    println(now)


  }
}
