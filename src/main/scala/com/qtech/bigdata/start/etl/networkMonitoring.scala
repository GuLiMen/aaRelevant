package com.qtech.bigdata.start.etl

import java.util
import java.util.regex.Pattern

import com.qtech.bigdata.start.source.readKuduTable.readKuduTableDF
import com.qtech.bigdata.util.date.getHdfsDataDate
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import com.qtech.bigdata.util.date.dateslot_test
import com.qtech.bigdata.util.date.systemTime
import com.qtech.bigdata.util.date.dataPutTime
import com.qtech.bigdata.util.date.now
import org.apache.spark.rdd.RDD
import com.qtech.bigdata.start.source.readHdfs.getFilesAndDirs
import org.apache.spark.{SparkConf, SparkContext}
import com.qtech.bigdata.common.sourceScheam.kuduMaster
import com.qtech.bigdata.common.sourceScheam.resultTableSchema

import scala.collection.mutable.ListBuffer

/**
  * AA日志监控
  */
object networkMonitoring {

  def main(args: Array[String]): Unit = {
    //TODO 创建spark对象
    val conf = new SparkConf().setAppName("AAnetworkMonitoring")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.port.maxRetries", "500")
    //    .setMaster("local[*]")

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
    val resultTable = "DWS_AA_IS_NWTWORK"
    //读取结果表
    val options = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> resultTable
    )

    noNetwork(spark, sc, resultTable)

    //TODO 关闭spark对象
    spark.close()
  }

  /**
    * 拿到当前小时网络OK的数据
    *
    * @param spark
    */
  def haveNetwoek(spark: SparkSession): RDD[(String, String, String, String, String, String, String)] = {

    readKuduTableDF(spark, "ADS_AA_IS_NWTWORK", "source1")
    readKuduTableDF(spark, "impala::ods_baseinfo.device_mapping", "source2")

    // 拿到当前机台最新的数据
    spark.sql(
      """
        |select * from (
        |select area,cob,eqid,uploadtime,is_network,is_production,row_number()over(partition by area,cob,eqid order by uploadtime desc) as num from source1
        |)y
        |where num = 1
      """.stripMargin).createOrReplaceTempView("middle1")

    //join得到机台号
    val sourceDF = spark.sql(
      """
        |select tmp1.area,tmp1.cob,tmp1.eqid,tmp1.uploadtime,tmp1.is_network,tmp1.is_production,tmp2.device_num from middle1 as tmp1 join source2 as tmp2 on  tmp1.eqid = tmp2.eid
      """.stripMargin)

    //转换中英文，过滤时间小于当前的

    Tuple2
    val sourceRDD: RDD[(String, String, String, String, String, String, String)] = sourceDF.rdd.map(item => {
      (
        item.getAs[String]("area"),
        item.getAs[String]("cob"),
        item.getAs[String]("eqid"),
        item.getAs[String]("device_num"),
        item.getAs[String]("uploadtime"),
        item.getAs[String]("is_network"),
        item.getAs[String]("is_production")
      )
    })
      .map(res => {
        var targetArea = ""
        var area = res._1
        var cob = res._2
        var eqid = res._3
        var device_num = res._4
        var uploadtime = res._5
        var is_network = res._6
        var is_production = res._7

        //转换数据里的英文。
        if (area == "TaiHong") {
          targetArea = "台虹"
        } else if (area == "GuCheng") {
          targetArea = "古城"
        } else if (area == "ChengBei" || area == "GuCheng2") {
          targetArea = "古二"
        }
        //返回初始化数据
        (targetArea, cob, eqid, device_num, uploadtime, is_network, is_production)
      }).filter(res => {
      //过滤掉小于当前小时的数据
      res._5.compareTo(systemTime()) > 0
    })
    sourceRDD
  }

  //输出没网数据
  def noNetwork(spark: SparkSession, sc: SparkContext, resultTable: String): Unit = {
    //此处隐士转换，用于后续转换DF
    val sql = spark.sqlContext
    import sql.implicits._
    //拿到两张表的数据
    readKuduTableDF(spark, "impala::ods_baseinfo.device_mapping", "tmp1")
    spark.createDataFrame(haveNetwoek(spark)).createOrReplaceTempView("tmp2")

    //join数据
    val join = spark.sql(
      """
        |select * from (
        |select area , cob, eid, device_num , time, is_network, is_production,row_number()over(partition by eid order by time) as num from (
        |select tmp1.area as area ,tmp1.cob as cob,tmp1.eid as eid,tmp1.device_num as device_num ,tmp2._5 as time,tmp2._6 as is_network,tmp2._7 as is_production from tmp1
        |left join tmp2 on tmp1.eid = tmp2._3
        |order by tmp1.area,tmp1.cob,tmp1.device_num
        |)H
        |)g
        |where num = 1
      """.stripMargin)
    //转换RDD
    val dataRDD = join.rdd.map(res => {
      (
        res.getAs[String]("area"),
        res.getAs[String]("cob"),
        res.getAs[String]("eid"),
        res.getAs[String]("device_num"),
        res.getAs[String]("time"),
        res.getAs[String]("is_network"),
        res.getAs[String]("is_production")
      )
    })

    val valueRDD = dataRDD.map(res => {
      var timeslot = ""
      var time = ""

      if (res._5 == null || res._5 == "null") {
        time = now()
      } else {
        time = res._5
      }

      //拿到上传数据的所属时间段
      timeslot = dateslot_test(now())

      (res._1, res._2, res._3, res._4, time, res._6, res._7, timeslot)
    }).filter(res => {
      //过滤测试机台，因测试机台没有上传AALot数据
      !res._4.contains("S")
    }).toDF("area", "cob", "eid", "device_num", "time", "is_network", "is_production", "timeslot")

    //拉取hdfs目录数据，转换成RDD
    val data: RDD[String] = sc.makeRDD(hdfsData())
    val hdfsRDD = data.map(res => {
      //切割从hdfs上的目录数据，
      val datas: Array[String] = res.split("、")
      (datas(0), datas(1), datas(2), datas(3), datas(4), datas(5))
    }).toDF("b_area", "b_cob", "b_device_num", "b_eid", "b_lot", "b_remarks")

    //上面数据与hdfs目录数据join，结合
    val dataJoin: DataFrame = valueRDD.join(hdfsRDD, hdfsRDD("b_eid") <=> (valueRDD("eid")), "left").drop("b_area", "b_cob", "b_device_num").orderBy("area", "cob", "eid")

    val resultRDD: Dataset[(String, String, String, String, String, String, String, String, String, String)] = dataJoin.map(res => {
      (
        res.getAs[String]("area"),
        res.getAs[String]("cob"),
        res.getAs[String]("eid"),
        res.getAs[String]("device_num"),
        res.getAs[String]("time"),
        res.getAs[String]("is_network"),
        res.getAs[String]("is_production"),
        res.getAs[String]("timeslot"),
        res.getAs[String]("b_eid"),
        res.getAs[String]("b_lot"),
        res.getAs[String]("b_remarks")
      )
    })
      .map(res => {
        //判断网络正常，且以生产的情况下，数据是否上传,若没上传，可能存在无更新最新版本软件，或者生产机台重装系统无安装软件等。
        var area = ""
        var lot = ""
        var remarks = ""
        if (res._6 == "网络OK" && res._7 == "当前时段已生产" && (res._10 == "null" || res._10 == null) && res._5.compareTo(dataPutTime()) > 0) {
          remarks = "采集软件异常"
        } else {
          remarks = res._11
        }
        //与丁辉商议，用数字代表状态，方便后续统计。0代表无网络与无生产，1代表有网络与当前时段已生产
        var network = ""
        if (res._6 != "null" && res._6 != "NULL" && res._6 != null || res._6 == "网络OK") {
          network = "1"
        } else {
          network = "0"
        }
        var production = ""
        if (res._7 == "当前时段已生产") {
          production = "1"
        } else {
          production = "0"
        }
        if (res._6 == "null" || res._6 == null) {
          lot = null
        } else {
          lot = res._10
        }
        if (res._1.equalsIgnoreCase("GT")) {
          area = "古二"
        } else if (res._1.equalsIgnoreCase("TH")) {
          area = "台虹"
        } else if (res._1.equalsIgnoreCase("GC")) {
          area = "古城"
        }
        (area, res._2, res._3, res._4, res._8, res._5, network, production, lot, remarks)
      }).where("_3 != 'EQ01000010330006' ")

    //写入结果表
    spark.createDataFrame(resultRDD.toDF().rdd, resultTableSchema)
      .write.mode(SaveMode.Append)
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", kuduMaster)
      .option("kudu.table", resultTable)
      .save()
  }

  //拉取hdfs数据，将其放入集合中
  private def hdfsData(): ListBuffer[String] = {

    //判断数字正则
    val res =
      """^(\d+)$"""
    val pattern = Pattern.compile(res)
    var set: util.Set[String] = new util.HashSet[String]
    var data: ListBuffer[String] = new ListBuffer[String]()

    //根据班次拿到数据。
    getFilesAndDirs("/flume", getHdfsDataDate()).foreach(item => {
      val datas: Array[String] = item.split("、")
      val area: String = datas(0)
      val eid: String = datas(3)
      val Lot: String = datas(4)
      val Lots: Array[String] = Lot.split("-")
      if (area.equalsIgnoreCase("古城")) {
        val deviceNum_1: String = Lots(0)
        val deviceNum_2: String = Lots(1)
        if (Lot.contains("--") || Lot.contains(" ") || Lot.contains("#") || pattern.matcher(s"$deviceNum_1").matches() == false || pattern.matcher(s"$deviceNum_2").matches() == false) {
          if (set.add(eid)) {
            data += item + "、" + "Lot错误"
          }
        }

        if (set.add(eid)) {
          data += item + "、" + "NULL"
        }
      } else if (area.equalsIgnoreCase("古二")) {
        val deviceNum_1: String = Lots(0)
        val deviceNum_2: String = Lots(1)
        if (Lot.contains("--") || Lot.contains(" ") || Lot.contains("#") || pattern.matcher(s"$deviceNum_1").matches() == false || pattern.matcher(s"$deviceNum_2").matches() == false) {
          if (set.add(eid)) {
            data += item + "、" + "Lot错误"
          }
        }

        if (set.add(eid)) {
          data += item + "、" + "NULL"
        }
      } else if (area.equalsIgnoreCase("台虹")) {
        val deviceNum: String = Lots(0)
        if (Lot.contains("--") || Lot.contains(" ") || Lot.contains("#") || pattern.matcher(s"$deviceNum").matches() == false) {
          if (set.add(eid) == true) {
            data += item + "、" + "Lot错误"
          }
        }
        if (set.add(eid)) {
          data += item + "、" + "NULL"
        }
      }

    })
    data
  }

}