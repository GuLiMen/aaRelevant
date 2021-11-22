package com.qtech.bigdata.start.etl

import com.qtech.bigdata.start.source.readKuduTable.readKuduTableDF
import com.qtech.bigdata.util.date.yesterday
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *  todo：抛料率厂区映射表维护程序
  */
object throwingRate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("AAnetworkMonitoring")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.port.maxRetries", "500")
//      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.default.parallelism", "3")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    //读取解析表，获取厂区机种时间
    readKuduTableDF(spark, "impala::ODS_PRODUCT_PROCESS.AA_REJECT_CB", "cb")
    readKuduTableDF(spark, "impala::ODS_PRODUCT_PROCESS.AA_REJECT_TH", "th")
    readKuduTableDF(spark, "impala::ODS_PRODUCT_PROCESS.AA_REJECT_GC", "gc")

    //读取结果表
    readKuduTableDF(spark, "ADS_AA_THROWINGRATE_MAPPING", "throwing")

    //拿到增量时间，每天运行前一天的
    val time: String = yesterday()

    //union 三个表，查询当前月度厂，区下机种的list
    spark.sql(
     s"""
        |select * from (
        |select area,cob,process_type,substr(main_aa_start,1,7) as time from cb
        |where main_aa_start >= '$time'
        |group by area,cob,process_type,time order by area,cob,process_type,time
        |)a
        |union
        |select * from (
        |select area,cob,process_type,substr(main_aa_start,1,7) as time from gc
        |where main_aa_start >= '$time'
        |group by area,cob,process_type,time order by area,cob,process_type,time
        |)b
        |union
        |select * from (
        |select area,cob,process_type,substr(main_aa_start,1,7) as time from th
        |where main_aa_start >= '$time'
        |group by area,cob,process_type,time order by area,cob,process_type,time
        |)c
      """.stripMargin).createOrReplaceTempView("tmp1")

    spark.sql(
      """
        |insert into throwing select * from tmp1
      """.stripMargin)

    spark.stop()
  }




}
