package com.qtech.bigdata.start.source

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import scala.collection.mutable.ListBuffer

/**
  * 返回HDFS上读取到的目录数据
  */
object readHdfs {

  //获取HDFS配置信息
  def getConf = {
    System.setProperty("HADOOP_USER_NAME", "qtkj")
    val conf = new Configuration
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    conf.set("fs.defaultFS", "hdfs://nameservice")
    conf.set("dfs.nameservices", "nameservice")
    conf.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02")
    conf.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020")
    conf.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020")
    conf.set("dfs.client.failover.proxy.provider.nameservice", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    conf
  }

  def getHdfs(path: String) = {
    val conf = getConf
    FileSystem.get(URI.create(path), conf)
  }

  //将上传数据存放集合中
  def getFilesAndDirs(path: String,date:String): ListBuffer[String] = {

    var data: ListBuffer[String] = new ListBuffer[String]()

    val fs: Array[FileStatus] = getHdfs(path).listStatus(new Path(path))
    //遍历HDFS路径，示例：/flume/TaiHong/COB3/EQ01000003300007/Lot/16-C0LA39-7-30-D
    for (factory: FileStatus <- fs) {
      for (cob: FileStatus <- getHdfs(path).listStatus(new Path(factory.getPath.toString))) {
        for (eid: FileStatus <- getHdfs(path).listStatus(new Path(cob.getPath.toString))) {
          for (lot: FileStatus <- getHdfs(path).listStatus(new Path(eid.getPath.toString))) {
            for (lotName <- getHdfs(path).listStatus(new Path(lot.getPath.toString))) {
              //定义厂，EQ，机台，区，Lot文件名变量
              var factory_a: String = factory.getPath.getName
              val eid_a: String = eid.getPath.getName
              var region: String = null
              val machine: String = cob.getPath.getName
              val name: String = lotName.getPath.getName
              if (name.contains(date)) {
                if (factory.getPath.getName.equals("GuCheng")) {
                  factory_a = factory_a.replace("GuCheng", "古城")
                  val names = name.split("-")
                  //因古城与其他厂规则不一
                  //得到线体-机台号
                  region = names {
                    0
                  }+"-"+ names {
                    1
                  }

                  data += factory_a + "、" + machine + "、" + region + "、" + eid_a + "、" + name

                } else if (factory.getPath.getName.equals("TaiHong")) {
                  factory_a = factory_a.replace("TaiHong", "台虹")
                  val names = name.split("-")
                  //得到机台号
                  region = names {
                    0
                  }

                  data += factory_a + "、" + machine + "、" + region + "、" + eid_a + "、" + name
                } else if (factory.getPath.getName.equals("GuCheng2")) {
                  factory_a = factory_a.replace("GuCheng2", "古二")
                  val names = name.split("-")
                  //因古城与其他厂规则不一
                  //得到线体-机台号
                  region = names {
                    0
                  }+"-"+ names {
                    1
                  }

                  data += factory_a + "、" + machine + "、" + region + "、" + eid_a + "、" + name

                }
              }
            }
          }
        }
      }
    }

    data

  }

}
