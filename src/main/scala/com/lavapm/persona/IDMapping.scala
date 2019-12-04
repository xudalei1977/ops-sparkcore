package com.lavapm.persona

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil


/**
  * ID Mapping
  * Created by dalei on 7/4/2018.
  */
object IDMapping {


  val sparkConf = new SparkConf().setAppName("ID Mapping")
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(sparkConf)
  val hbaseConf = HBaseConfiguration.create()
  val sqlContext = new SQLContext(sc)

  val connection = ConnectionFactory.createConnection(hbaseConf)



  /**
    * check whether the encrypted imei exists in persona:user_id_info in hbase
    * return: lava_id if exists
    *         "" if not exists
    */
  def checkImeiExistInUserIDInfo(imeiEnc: String): String = {

    val tableName = "persona:user_id_info"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val table = connection.getTable(TableName.valueOf(tableName))
    val scan = new Scan()

    // add the filter
    val filterArr = new java.util.ArrayList[Filter](1)
    var compareOp = CompareFilter.CompareOp.EQUAL

    val filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"),
                                              Bytes.toBytes("imei_enc"),
                                              compareOp,
                                              new BinaryComparator(Bytes.toBytes(imeiEnc)))
    filter1.setFilterIfMissing(true)
    filterArr.add(filter1)

    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterArr)
    scan.setFilter(filterList)

    // add the scan to hbaseConf
    hbaseConf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan))

    // get the original rdd
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf,
                                      classOf[TableInputFormat],
                                      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                      classOf[org.apache.hadoop.hbase.client.Result])

    // build the result rdd
    val rowRDD = hbaseRDD.map { case (_, result) => {
      // get the rowkey
      val key = Bytes.toString(result.getRow)
      println("******************* key := " + key)
      return key
    }
    }

    ""
  }

  /**
    * merge the user_id_info in hive raw database to persona:user_id_info in hbase
    */
  def mergeUserIDInfo(imeiEnc: String): String = {

    val tableName = "persona:user_id_info"
    val conf = HBaseConfiguration.create()

    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    println("******************* hBaseRDD.count() := " + hBaseRDD.count())

    hBaseRDD.foreach{ case (_,result) => {

      //获取行键
      val key = Bytes.toString(result.getRow)

      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("info".getBytes, "imei_enc".getBytes))
      val age = Bytes.toInt(result.getValue("info".getBytes, "age_range".getBytes))
      println("******************* Row key : " + key + " Name : " + name + " Age : " + age)
    }}

    sc.stop()

    ""
  }
}
