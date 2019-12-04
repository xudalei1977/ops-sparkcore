package com.lavapm.persona


import org.apache.spark.sql.SparkSession


/**
  * Client data map local data and extend more fields.
  * Create by Ryan on 05/08/2019.
  */
object ClientDataMapping {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("client data mapping")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val batch_no = args(0)
    val tableName = args(1)
    val hdfsPath = s"/user/dmp_app_user/data_analysis/"+ tableName +"/"+ batch_no +""


    if (args.length == 2)
      idMapping(spark,hdfsPath,null,tableName,batch_no)


    if (args.length == 3) {
      val parameters = args(2).split("\\|").toList
      idMapping(spark,hdfsPath,parameters,tableName,batch_no)
    }

    spark.stop()
  }


  def idMapping (spark:SparkSession,
                 hdfsPath:String,
                 parameters:List[String],
                 tableName:String,
                 batch_no:String) = {

    val idSet = Set("imei","imei_enc","idfa","idfa_enc","mobile","mobile_enc","email","email_enc")

    val dataFrame = spark
      .read
      .format("json")
      .load(hdfsPath)
      //.filter("_corrupt_record is null")


    val columnsArr = dataFrame.columns

    if(!spark.catalog.tableExists("data_analysis."+tableName)) {
      val createSQL = new StringBuilder("create table data_analysis."+ tableName +"(")

      for (column <- columnsArr) {
        createSQL.append("`" + column + "` string, ")
      }

      if (parameters != null) {
        createSQL.append("lava_id string, ")

        for (parameter <- parameters) {
          createSQL.append(parameter + " string, ")
        }
      }

      println("***createSQL***: "+createSQL.substring(0, createSQL.length - 2) + ") partitioned by(`batch_no` string) stored as parquet")
      spark.sql(createSQL.substring(0, createSQL.length - 2) + ") partitioned by(`batch_no` string) stored as parquet")
    }

    dataFrame.createOrReplaceGlobalTempView("client_table")

    if (parameters == null) {
      spark.sql("insert overwrite table data_analysis."+ tableName +" partition(batch_no='"+ batch_no +"') select * from global_temp.client_table")
    } else {
      val insertSQL = new StringBuilder("insert overwrite table data_analysis."+ tableName +" partition(batch_no='"+ batch_no +"') select a.*,b.lava_id")

      for (parameter <- parameters) {
        insertSQL.append(",b."+ parameter)
      }

      insertSQL.append(" from global_temp.client_table a left outer join persona.user_info b on ")

      for (column <- columnsArr) {
        if (idSet.contains(column)){
          insertSQL.append("(trim(b."+ column +") != '' and a."+column+" = b."+column+") or ")
        }
      }

      println("***insertSQL***: "+insertSQL.substring(0,insertSQL.length-3))
      spark.sql(insertSQL.substring(0, insertSQL.length - 3))
    }


    /*dataFrame.createOrReplaceGlobalTempView("sales_table")

    val sql = new StringBuilder("")

    if(!spark.catalog.tableExists("data_analysis."+tableName)) {
      sql.append("insert into table data_analysis."+ tableName +" select a.*,b.lava_id")
    }else {
      sql.append("create table data_analysis."+ tableName +" stored as parquet as select a.*,b.lava_id")
    }

    for (parameter <- parameters) {
      sql.append(",b."+ parameter +"")
    }

    sql.append(" from global_temp.sales_table a left outer join persona.user_info b on ")



    for (column <- columnsArr) {
      if (idSet.contains(column))
        sql.append("(trim(b."+ column +") != '' and a."+column+" = b."+column+") or ")
    }


    println("***sql***:"+sql.substring(0,sql.length-3))

    spark.sql(sql.substring(0, sql.length - 3))*/

  }


}
