package com.lavapm.dmp

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil


/**
  * Meta
  * Created by dalei on 7/16/2018.
  */
object Meta {

  /**
    * @param spark          spark session
    * @param namespace      namespace
    * @param tableName      table name
    * @param columnNameStr  column name, more columns departed by '#', for example: gender#age_range
    * @return enumJson      enum for each column, for example: {"column":"gender", "enum"}
    */
  def generateAudienceByTag(spark: SparkSession,
                            namespace: String,
                            tableName: String,
                            columnNameStr: String): String = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, namespace + ":" + tableName)

    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf(namespace + ":" + tableName))
    val scan = new Scan()
    // scan.setFilter(null)

    // use structType for dataFrame
    var listCol: List[StructField] = List()
    listCol :+= StructField("lava_id", StringType, true)

    val showCol = columnNameStr.split("#")
    for (col <- showCol) {
      listCol :+= StructField(col, StringType, true)
    }

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

      for (row <- showCol) {
        valueSeq :+= Bytes.toString(result.getValue(row.getBytes, row.getBytes))
      }
      Row.fromSeq(valueSeq)
    }
    }

    rowRDD.take(10)
    val hbaseDF = spark.createDataFrame(rowRDD, schema)

    hbaseDF.createOrReplaceTempView(tableName.replace(':', '_'))

    ""
  }


}
