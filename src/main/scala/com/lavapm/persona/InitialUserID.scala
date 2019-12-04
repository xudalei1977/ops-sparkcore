package com.lavapm.persona

import com.lavapm.utils.HBase
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil



/**
  * Create multiple hbase table to content the ID pair, e.g {imei_enc : lava_id}
  * this class should be executed after ProcessRawData.scala and ProcessAdxData.scala
  * Created by dalei on 10/24/18.
  */
object InitialUserID {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Initial User ID")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    println("**************** load the imei_enc to hbase table.")
    loadData2HbaseTable(sc, "imei_enc")

    println("**************** load the imei to hbase table.")
    loadData2HbaseTable(sc, "imei")

    println("**************** load the idfa to hbase table.")
    loadData2HbaseTable(sc, "idfa")

    println("**************** load the idfa to hbase table.")
    loadData2HbaseTable(sc, "idfa_enc")

    sc.stop()
  }



  /**
    * load data from persona:user_info to specific hbase table
    * @param sc             spark context
    * @param columnName     column name
    */
  def loadData2HbaseTable( sc: SparkContext,
                           columnName: String) : Unit = {

    // create the hbase table
    HBase.createTable("persona", columnName, Array("id"))

    // start to load data to the hbase data
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "persona:user_info")

    val scan = new Scan()
    val filterArr = new java.util.ArrayList[Filter](2)

    // add the imei_enc as filter
    val filter = new SingleColumnValueFilter(Bytes.toBytes("id"), Bytes.toBytes(columnName),
                                              CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("")))
    filter.setFilterIfMissing(true)
    filterArr.add(filter)

    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterArr)
    scan.setFilter(filterList)

    // add the column to scan
    // scan.addFamily(Bytes.toBytes("info"))
    scan.addColumn(Bytes.toBytes("id"), Bytes.toBytes(columnName))

    // add the scan to hbaseConf
    hbaseConf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan))

    // get the original rdd
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf,
                                      classOf[TableInputFormat],
                                      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                      classOf[org.apache.hadoop.hbase.client.Result])

    //println("*********** rdd.count = " + hbaseRDD.count)

    // build the result rdd from persona:user_info
    val rdd = hbaseRDD.map { case (_, result) => {

      val put = new Put(result.getValue("id".getBytes, columnName.getBytes))
      put.addColumn("id".getBytes, "lava_id".getBytes, result.getRow)

      (new ImmutableBytesWritable, put)
    }
    }

    //println("*********** rdd.count = " + rdd.count)

    // write the rdd to hbase table
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "persona:" + columnName)
    lazy val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
  }


}
