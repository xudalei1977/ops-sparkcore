package com.lavapm.dmp


import java.util

import com.fasterxml.jackson.annotation.JsonProperty

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, ResultScanner, Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.log4j.Logger
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.CompareOperator


import scala.beans.BeanProperty
import scala.collection.JavaConversions._


/**
  * Tag
  * Created by dalei on 7/5/2018.
  */
object Tag {

  val logger = Logger.getLogger(Tag.getClass)

  //过滤字段实体类
  case class Column(@JsonProperty("column_name") @BeanProperty column_name: String,@JsonProperty("column_family") @BeanProperty column_family: String, @JsonProperty("compare_operator") @BeanProperty compare_operator: String,@JsonProperty("column_type") @BeanProperty column_type: String,@JsonProperty("value") @BeanProperty value: String)
  case class Table(@JsonProperty("table_name") @BeanProperty table_name: String,@JsonProperty("filter") @BeanProperty columns: util.ArrayList[Column])
  case class Filters(@JsonProperty("filters") @BeanProperty filters: util.ArrayList[Table])
  //展示字段实体类
  case class ShowColumn(@JsonProperty("column_name") @BeanProperty column_name: String,@JsonProperty("column_family") @BeanProperty column_family: String)
  case class ShowTable(@JsonProperty("table_name") @BeanProperty table_name: String,@JsonProperty("show_column") @BeanProperty show_column: util.ArrayList[ShowColumn])
  case class ShowColumns(@JsonProperty("show_columns") @BeanProperty show_columns: util.ArrayList[ShowTable])



  case class interest(lava_id:String, age_range:String, category_name_l1:String)

  /**
    * create the hive table to add lava_id column to raw table
    * @param spark            spark session
    * @param tableName        table name
    * @param showCol          columns to show
    * @param filterCol        columns used in filter
    * @param prefix           prefix for the result record
    * @param outputPath       output path for the result record
    */
  def generateAudienceByTag(spark: SparkSession,
                            tableName: String,
                            showCol: ShowColumns,
                            filterCol: Filters,
                            prefix: String,
                            outputPath: String) = {

    val hbaseConf = HBaseConfiguration.create()

    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.setInt("hbase.rpc.timeout",20000);
    hbaseConf.setInt("hbase.client.operation.timeout",60000);
    hbaseConf.setInt("hbase.client.scanner.timeout.period",200000);

    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf(tableName))
    val scan = new Scan()
    scan.setMaxResultSize(100000)

    // set the rowkey range
    // scan.setStartRow(Bytes.toBytes(""))
    // scan.setStopRow(Bytes.toBytes(""))

    val filterColumnLen = filterCol.filters.size()

    // add the filter
    if (filterColumnLen > 0) {

      val filterArr = new java.util.ArrayList[Filter](filterColumnLen)
      var compareOp = CompareOperator.NOT_EQUAL
      for(tables <- filterCol.filters) {
        for (column <- tables.columns) {
          column.compare_operator match {
            case "=" => compareOp = CompareOperator.EQUAL
            case "<" => compareOp = CompareOperator.LESS
            case "<=" => compareOp = CompareOperator.LESS_OR_EQUAL
            case ">" => compareOp = CompareOperator.GREATER
            case ">=" => compareOp = CompareOperator.GREATER_OR_EQUAL
            case "!=" => compareOp = CompareOperator.NOT_EQUAL
            case _ => compareOp = CompareOperator.LESS
          }
          val filter1 = new SingleColumnValueFilter(Bytes.toBytes(column.column_family),
                                                    Bytes.toBytes(column.column_name),
                                                    compareOp,
                                                    new BinaryComparator(Bytes.toBytes(column.value)))
          filter1.setFilterIfMissing(true)
          filterArr.add(filter1)
        }
      }

      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterArr)
      scan.setFilter(filterList)

    } else {
      scan.setFilter(null)
    }

    // val prefix = "'\"tenant_id\":\"lavapm\",\"crowd_id\":"+crowd_id+",\"object_code\":\"METAACCOUNT20180530001\",\"batch_no\":\"lavapm\"'"

    // compose the sql sentence
    var prefixSql = prefix.replace('{', ' ').trim()
    prefixSql = prefixSql.replace('}', ' ').trim()
    var sql = "select concat('{', " + "'" + prefixSql + "'"

    // use structType for dataFrame
    var listCol: List[StructField] = List()
    listCol :+= StructField("lava_id", StringType, true)

    //定义展示字段的StructField
    for(columns <- showCol.show_columns){
      for(column <- columns.show_column) {
        listCol :+= StructField(column.column_name, StringType, true)
        //        sql += ", ', \"" + i._2 + "\":\"', nvl(" + i._2 + ", 'NULL'), '\"' "
        sql += ", ',\"" + column.column_name + "\":\"', nvl(" + column.column_name + ", 'NULL'),'\"'"
      }
    }

    sql += ", '}') from " + tableName.replace(':', '_')

    logger.info("*********************** sql := " + sql)

    // build the structType
    val schema = StructType(listCol)

    // add the scan to hbaseConf
    hbaseConf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan))

    // get the original rdd
    val sc = spark.sparkContext
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf,
                                      classOf[TableInputFormat],
                                      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                      classOf[org.apache.hadoop.hbase.client.Result])

    // build the result rdd
    val rowRDD = hbaseRDD.map { case (_, result) => {
      var valueSeq: Seq[String] = Seq()
      // get the rowkey
      val key = Bytes.toString(result.getRow)
      valueSeq :+= key

      //get the value
      for(columns <- showCol.show_columns) {
        for(column <- columns.show_column){
          valueSeq :+= Bytes.toString(result.getValue(column.column_family.getBytes(), column.column_name.getBytes()))
        }
      }
      Row.fromSeq(valueSeq)
    }
    }

    // register to schema
    val hbaseDF = spark.createDataFrame(rowRDD, schema)
    hbaseDF.createGlobalTempView(tableName.replace(':', '_'))
    spark.sql(sql).limit(100000).rdd.map(x => x.get(0)).saveAsTextFile("/user/dmp_app_user/tag"+outputPath)
  }

  def main(args: Array[String]): Unit = {

    var filter = args(0)
    var show = args(1)
    var head = args(2)
    var outputPath = args(3)

    logger.info("---------接收传递参数filter："+filter)
    logger.info("---------接收传递参数show: "+show)
    logger.info("---------接收传递参数head: "+head)
    logger.info("---------接收传递参数outputPath: "+outputPath)


    import com.fasterxml.jackson.core.JsonParser.Feature
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.databind.DeserializationFeature

    val mapper = new ObjectMapper
    //解析器支持解析单引号
    mapper.configure(Feature.ALLOW_SINGLE_QUOTES, true)
    //解析器支持解析结束符
    mapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val jsonFilter = mapper.readValue(filter, classOf[Filters]) //转换为HashMap对象
    val jsonShow = mapper.readValue(show, classOf[ShowColumns])

    var tableName = jsonFilter.filters.get(0).table_name
    logger.info("*********************** before show tables tableName："+tableName)
    tableName = tableName.replace(".",":")

    val spark = SparkSession
      .builder()
      .appName("Tag")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    generateAudienceByTag(spark, tableName, jsonShow, jsonFilter,head,outputPath)

    spark.stop
  }
}
