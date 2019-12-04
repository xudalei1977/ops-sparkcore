package com.lavapm.dmp


import com.lavapm.utils.Json
import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, ResultScanner, Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil



/**
  * Tag
  * Created by dalei on 7/5/2018.
  */
object Tag_bak {

  val recordLimit = 100000

  val sparkConf = new SparkConf().setAppName("Tag")
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(sparkConf)
  val hbaseConf = HBaseConfiguration.create()
  val sqlContext = new SQLContext(sc)


//  def convertScanToString(scan: Scan) = {
//    val proto = ProtobufUtil.toScan(scan)
//    Base64.encodeBytes(proto.toByteArray)
//  }


  /**
    * @param relationOper   relation operator (AND, OR)
    * @param tableName      table name
    * @param sqlHead        head part in sql sentence
    * @param hdfsPath       hdfs path for the result
    * @param filterCol      _1 column family  _2 column name  _3 filter value -4 compare type (=, <, >, !=...)
    * @param showCol        _1 column family  _2 column name  _3 column type (String, Int, Double, Timestamp...)
    */
  def generateAudienceByTag(relationOper: String,
                            tableName: String,
                            sqlHead: String,
                            hdfsPath: String,
                            filterCol: Array[(String, String, String, String)],
                            showCol: Array[(String, String, String)]) = {

    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()

    // set the rowkey range
    // scan.setStartRow(Bytes.toBytes(""))
    // scan.setStopRow(Bytes.toBytes(""))

    val filterColumnLen = filterCol.length

    // add the filter
    if (filterColumnLen > 0) {

      val filterArr = new java.util.ArrayList[Filter](filterColumnLen)
      var compareOp = CompareFilter.CompareOp.NOT_EQUAL

      for (i <- filterCol) {

        i._4 match {
          case "=" =>   compareOp = CompareFilter.CompareOp.EQUAL
          case "<" =>   compareOp = CompareFilter.CompareOp.LESS
          case "<=" =>  compareOp = CompareFilter.CompareOp.LESS_OR_EQUAL
          case ">" =>   compareOp = CompareFilter.CompareOp.GREATER
          case ">=" =>   compareOp = CompareFilter.CompareOp.GREATER_OR_EQUAL
          case "!=" =>   compareOp = CompareFilter.CompareOp.NOT_EQUAL
          case _ => compareOp = CompareFilter.CompareOp.LESS
        }

        val filter1 = new SingleColumnValueFilter(Bytes.toBytes(i._1),
          Bytes.toBytes(i._2),
          compareOp,
          new BinaryComparator(Bytes.toBytes(i._3)))
        filter1.setFilterIfMissing(true)
        filterArr.add(filter1)
      }

      if (relationOper.equalsIgnoreCase("AND")) {
        val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterArr)
        scan.setFilter(filterList)
      } else {
        val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filterArr)
        scan.setFilter(filterList)
      }

    } else {
      scan.setFilter(null)
    }

    // add the scan to hbaseConf
    hbaseConf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan))

    // get the original rdd
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

      for (row <- showCol) {
        valueSeq :+= Bytes.toString(result.getValue(row._1.getBytes, row._2.getBytes))
      }
      Row.fromSeq(valueSeq)
    }
    }

    // compose the sql sentence
    var sql = "select concat('{', " + sqlHead

    // use structType for dataFrame
    var listCol: List[StructField] = List()
    listCol :+= StructField("lava_id", StringType, true)

    for (i <- showCol) {
      listCol :+= StructField(i._2, StringType, true)
      sql += ", ', \"" + i._2 + "\":\"', nvl(" + i._2 + ", 'NULL'), '\"' "
    }

    sql += ", '}') from " + tableName.replace(':', '_')

    println("*********************** sql := " + sql)

    // build the structType
    val schema = StructType(listCol)

    // register to schema
    val hbaseDF = sqlContext.createDataFrame(rowRDD, schema)
    hbaseDF.registerTempTable(tableName.replace(':', '_'))
    //sqlContext.sql(sql).show(10)
    sqlContext.sql(sql).limit(recordLimit).rdd.map(x => x.get(0)).saveAsTextFile(hdfsPath)
  }


  def main(args: Array[String]): Unit = {

    println("**********" + args.length)
    println("**********" + args(0))

    if (args.length != 4){
      println("Usage: Tag <filter json string> <show columns json string> <json head> <hdfs path>")
      sys.exit(1)
    }

    //    val a = new Array[Tuple3[Int, String, String]](2)
    //    val show_col = Array(("info", "age_range", "String"),("info", "category_name_l1", "String"))
    //    val filter_col = Array(("info", "gender", "F", "="))
    //    val head = "'\"tenant_id\": \"lavapm\",\"crowd_id\": 123, \"object_code\": \"METAACCOUNT20180530001\",\"batch_no\": \"lavapm\"'"

    val (relationOper, tableName, filter_col) = Json.getTableNameAndFilterFromJsonString(args(0))
    val (retStr, show_col) = Json.getTableNameAndShowColumnFromJsonString(args(1))

    println("*********************** relationOper := " + relationOper)
    println("*********************** tableName := " + tableName)
    println("*********************** args(2) := " + args(2))
    println("*********************** args(3) := " + args(3))
    //println("*********************** filter_col := " + filter_col)
    //println("*********************** show_col := " + show_col)
    generateAudienceByTag(relationOper, tableName, args(2), args(3), filter_col, show_col)

    sc.stop
  }
}
