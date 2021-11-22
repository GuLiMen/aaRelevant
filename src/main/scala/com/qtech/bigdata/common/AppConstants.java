package com.qtech.bigdata.common;

import java.util.List;

import static java.util.Arrays.asList;

//此类没有用
public class AppConstants {
    // AA机Log写入phoenix库表
    public static final String AALOG_SINK_PHOENIX_SCHEMA = "ODS_PRODUCT_PROCESS";
    public static final String AALOG_SINK_PHOENIX_TABLE_TH = "AA_LOG_TH";
    public static final String AALOG_SINK_PHOENIX_TABLE_CB = "AA_LOG_CB";
    public static final String AALOG_SINK_PHOENIX_TABLE_GC = "AA_LOG_GC";

    public static final String PHOENIX_DUTY_DATABASE ="ODS_BASE";
    public static final String PHOENIX_DUTY_TABLE ="DEVICE_INFO";

    public static final String KUDU_MASTER = "bigdata01,bigdata02,bigdata03";

    // phoenix
    //public static final String PHOENIX_URL = "jdbc:phoenix:10.170.6.133,10.170.6.134,10.170.6.135:2181";
    public static final String PHOENIX_URL = "jdbc:phoenix:bigdata01,bigdata02,bigdata03:2181";

    //mysql
 /*   public static final String MYSQL_HOST ="10.170.6.160";
    public static final String MYSQL_PORT ="3306";
    public static final String MYSQL_USER ="ziyunIot";
    public static final String MYSQL_PASSWD ="Pass1234";
    public static final String MYSQL_DATABASE ="ziyun-iot";
    public static final String MYSQL_TABLE ="t_device_calcgd1jh1u82gwwionk";*/


    // 插入失败
    public static final String PHOENIX_UPSERT_ERROR_CODE = "001001";
    public final static String PHOENIX_UPSERT_ERROR_MESSAGE = "插入语句失败!!";

    public final static String PHOENIX_ALTER_ERROR_CODE = "001002";
    public final static String PHOENIX_ALETR_ERROR_MESSAGE = "建表 或 修改表结构失败!!";

    public final static String PHOENIX_SELECT_ERROR_CODE = "001003";
    public final static String PHOENIX_SELECT_ERROR_MESSAGE = "查询失败 !!";

    /**
     * AA-list 接收邮箱
     */
    public final static List<String> RECEIVE_EMAIL = asList("limeng.gu@qtechglobal.com");

    public final static String IMPALA_JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    public final static String IMPALA_CONNECTION_URL = "jdbc:impala://10.170.3.12:21050/erp_job;UseSasl=0;AuthMech=3;UID=qtkj;PWD=qt_qt";

    public final static List<String> RECEIVE_MAIL_EQUIPMENT = asList("xiaokang.yan@qtechglobal.com","yan.liu@qtechglobal.com","yunhu.li@qtechglobal.com","gaoyan.yang@qtechglobal.com",
            "chuanliang.lv@qtechglobal.com","zengming.tan@qtechglobal.com","fengcheng.jiao@qtechglobal.com","fangben.ma@qtechglobal.com","wenlong.zhao@qtechglobal.com","mingbo.li@qtechglobal.com",
            "lufeng.kong@qtechglobal.com","hongbin.jia@qtechglobal.com","yangguang.yan@qtechglobal.com","ming.cao@qtechglobal.com","yongmeng.ma@qtechglobal.com","shuaishuai.jiao@qtechglobal.com",
            "dongdong.li_cob@qtechglobal.com","weijian.wang@qtechglobal.com","fengjin.yang@qtechglobal.com","fengxiang.sun@qtechglobal.com","xiuhao.huang@qtechglobal.com",
            "mingsheng.zhao@qtechglobal.com","shuo.zhang_shebei@qtechglobal.com","zongpu.huang@qtechglobal.com","peng.lin_cob@qtechglobal.com","honggen.yang@qtechglobal.com",
            "jinpeng.yu@qtechglobal.com","sheng.luo@qtechglobal.com","tao.pan_gc@qtechglobal.com","renfeng.cai@qtechglobal.com","zhiqiang.fan@qtechglobal.com","zhen.zuo@qtechglobal.com","xiangping.luo@qtechglobal.com",
            "haixue.xiang@qtechglobal.com","xianfu.hu@qtechglobal.com","xishuang.yan@qtechglobal.com","limeng.gu@qtechglobal.com","anliang.wang@qtechglobal.com","chunjiang.peng@qtechglobal.com","zhengqu.cao@qtechglobal.com",
            "guang.shi@qtechglobal.com","xiaosheng.wang@qtechglobal.com","bobing.ding@qtechglobal.com","hongfei.peng@qtechglobal.com","yong.liu@qtechglobal.com");
}