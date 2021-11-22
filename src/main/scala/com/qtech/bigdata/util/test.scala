package com.qtech.bigdata.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}

object test {
  def main(args: Array[String]): Unit = {
    //TODO 创建spark对象
    val conf = new SparkConf().setAppName("AAnetworkMonitoring")
      .set("spark.debug.maxToStringFields", "100")
//      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.default.parallelism", "3")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("warn")
    sc.hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nameservice", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    readKuduTableOrView(spark,"MESC_TEST_MSR","MESC_TEST_MSR")

    spark.sql(
      """
        |select * from MESC_TEST_MSR
      """.stripMargin).show(128)

    //TODO 关闭spark对象
    spark.close()
  }

  def readKuduTableOrView(spark: SparkSession, areaTable: String,areaView:String): Unit = {
    spark.read.format("kudu")
      .options(Map("kudu.master" -> "bigdata01,bigdata02,bigdata03",
        "kudu.table" -> areaTable))
      .load
      .createOrReplaceTempView(areaView)
  }


}
