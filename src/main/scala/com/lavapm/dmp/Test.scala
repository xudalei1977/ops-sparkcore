package com.lavapm.dmp


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}




/**
  * Test
  * Created by dalei on 4/18/19.
  */
object Test {

  val conf = new SparkConf().setAppName("SparkPutByMap")
  val sc = new SparkContext(conf)

  val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "persona:test1")

  lazy val job = Job.getInstance(hbaseConf)
  job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
  job.setOutputKeyClass(classOf[ImmutableBytesWritable])
  job.setOutputValueClass(classOf[Result])

  val rdd = sc.makeRDD(1 to 100)

  rdd.map(value => {
    val put = new Put(Bytes.toBytes(value.toString))
    put.addColumn(Bytes.toBytes("lf"), Bytes.toBytes("c1"), Bytes.toBytes("value1"))
    (new ImmutableBytesWritable(Bytes.toBytes(value.toString)), put)
  }).saveAsNewAPIHadoopDataset(job.getConfiguration)


}
