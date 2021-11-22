package com.qtech.bigdata.start.etl

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.qtech.bigdata.common.sourceScheam.kuduMaster
object dimensionalModeling {
  def main(args: Array[String]): Unit = {


    //TODO 创建spark对象
    val conf = new SparkConf().setAppName("AAdimensionalModeling")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.port.maxRetries", "500")
        .setMaster("local[*]")

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

    //todo 开始处理数据
    val resultTable = "impala::ODS_PRODUCT_PROCESS.AA_REJECT_TH"
    //读取结果表
    val options = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> resultTable
    )

    spark.sql(
      """
        |select area,cob,eid,device_num from aa_reject_th where main_aa_start > '2021-10-01'
        |group by area,cob,eid,device_num
        |grouping sets((area),(area,cob))
      """.stripMargin).show(128)



  }

}
