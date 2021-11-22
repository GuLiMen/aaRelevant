package com.qtech.bigdata.start.synchronization

import com.qtech.bigdata.util.readOracleView
import org.apache.spark.sql.{SaveMode, SparkSession}

object oa extends readOracleView{

  val KUDU_MASTER = "bigdata01,bigdata02,bigdata03"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("OA")
//      .master("local[*]")
      .config("spark.debug.maxToStringFields", "200")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

//    OA_EKP_ITRBP_LAUNCH_INFO(spark)
//    oa_ekp_itrbp_detail_info(spark)
    oa_km_review_main(spark)
    oa_lbpm_audit_note(spark)
    oa_lbpm_process(spark)

    spark.stop()

  }

  def OA_EKP_ITRBP_LAUNCH_INFO(spark:SparkSession):Unit = {
    var resultTable = "OA_EKP_ITRBP_LAUNCH_INFO"

    oa_table(spark,"EKP_ITRBP_LAUNCH_INFO","tmp1")

    kuduTable(spark,resultTable,"OA_EKP_ITRBP_LAUNCH_INFO")

    val targetDF = spark.sql(
      """
        |select
        |cast(FD_ID      as  STRING),
        |cast(FD_SUBMITER           as  STRING),
        |cast(FD_SUB_DEPT           as  STRING),
        |cast(FD_SUB_TIME           as  STRING),
        |cast(FD_LSSUE_NAME       as  STRING),
        |cast(FD_LSSUE_NAME_TEXT              as  STRING),
        |cast(FD_LSSUE_CODE       as  STRING),
        |cast(FD_LSSUE_CODE_TEXT              as  STRING),
        |cast(FD_LSSUE_TYPE       as  STRING),
        |cast(FD_LSSUE_TYPE_TEXT              as  STRING),
        |cast(FD_AREA  as  STRING),
        |cast(FD_WORK_SECTION   as  STRING),
        |cast(FD_WORKSHOP           as  STRING),
        |cast(FD_WORKSHOP_TEXT as  STRING),
        |cast(FD_CUSTOMER           as  STRING),
        |cast(FD_MACHINE_TYPE   as  STRING),
        |cast(FD_MACHINE_TYPE_TEXT          as  STRING),
        |cast(FD_PROJECT_PHASE as  STRING),
        |cast(FD_INPUTI_SOURCE as  STRING),
        |cast(FD_INPUTI_SOURCE_TEXT        as  STRING),
        |cast(FD_INPUTII_SOURCE                as  STRING),
        |cast(FD_INPUTII_SOURCE_TEXT      as  STRING),
        |cast(FD_OCCURRENCE_TIME              as  STRING),
        |cast(FD_EXCEP_STATION as  STRING),
        |cast(FD_PRG_NUMBER       as  FLOAT),
        |cast(FD_PRG_RACE           as  STRING),
        |cast(FD_ISSUE_DESC       as  STRING),
        |cast(FD_JOB_NUM             as  STRING),
        |cast(FD_PROC_NUM           as  STRING),
        |cast(FD_DQA_ENGINEER   as  STRING),
        |cast(FD_NPI_ENGINEER   as  STRING),
        |cast(FD_ACCIDENT_MANAGER            as  STRING),
        |cast(FD_ANALYSIS_RESULTS            as  STRING),
        |cast(FD_READY_PLAN_USER              as  STRING),
        |cast(FD_READY_PLAN_AREA              as  STRING),
        |cast(FD_RECOVERY           as  STRING),
        |cast(FD_OPINION_CA       as  STRING),
        |cast(FD_OPINION_CB       as  STRING),
        |cast(FD_EXCEPTION_LEVEL              as  STRING),
        |cast(FD_OPINION_LA       as  STRING),
        |cast(FD_OPINION_LB       as  STRING),
        |cast(FD_MEASURES_A       as  STRING),
        |cast(FD_MEASURES_R       as  STRING)
        |from tmp1
      """.stripMargin)

    targetDF.write.mode(SaveMode.Append)
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTER)
      .option("kudu.table", resultTable)
      .save()

  }

  def oa_ekp_itrbp_detail_info(spark:SparkSession):Unit = {
    var resultTable = "OA_EKP_ITRBP_DETAIL_INFO"

    oa_table(spark,"ekp_itrbp_detail_info","tmp1")

    kuduTable(spark,resultTable,"EKP_ITRBP_DETAIL_INFO")

    val targetDF = spark.sql(
      """
        |select
        |cast(FD_ID as STRING),
        |cast(FD_SUBMITER as STRING),
        |cast(FD_SUB_DEPT as STRING),
        |cast(FD_SUB_TIME as STRING),
        |cast(FD_LSSUE_CODE as STRING),
        |cast(FD_LSSUE_NAME as STRING),
        |cast(FD_LSSUE_TYPE as STRING),
        |cast(FD_AREA as STRING),
        |cast(FD_WORK_SECTION as STRING),
        |cast(FD_WORKSHOP as STRING),
        |cast(FD_CUSTOMER as STRING),
        |cast(FD_MACHINE_TYPE as STRING),
        |cast(FD_PROJECT_PHASE as STRING),
        |cast(FD_EXCEPTION_INPUT_SOURCE as STRING),
        |cast(FD_EXCEPTION_INPUTI_SOURCE as STRING),
        |cast(FD_OCCURRENCE_TIME as STRING),
        |cast(FD_EXCEPTION_STATION as STRING),
        |cast(FD_PRG_NUMBER as STRING),
        |cast(FD_PRG_RACE as STRING),
        |cast(FD_ISSUE_DESC as STRING),
        |cast(FD_JOB_NUMBER as STRING),
        |cast(FD_PROC_NUM as STRING),
        |cast(FD_DQA_ENGINEER as STRING),
        |cast(FD_NPI_ENGINEER as STRING),
        |cast(FD_ACCIDENT_MANAGER as STRING),
        |cast(FD_ANALYSIS_RESULTS as STRING),
        |cast(FD_READY_PLAN_USER as STRING),
        |cast(FD_READY_PLAN_AREA as STRING),
        |cast(FD_RECOVERY as STRING),
        |cast(FD_OPINION_CA as STRING),
        |cast(FD_OPINION_CB as STRING),
        |cast(FD_EXCEPTION_LEVEL as STRING),
        |cast(FD_OPINION_LA as STRING),
        |cast(FD_OPINION_LB as STRING),
        |cast(FD_MEASURES_A as STRING),
        |cast(FD_MEASURES_R as STRING),
        |cast(FD_RELATIVE_ID as STRING),
        |cast(FD_SUPERS_ID as STRING),
        |cast(FD_QUES_PERSON as STRING),
        |cast(FD_QUES_C_PERSON as STRING),
        |cast(FD_LEVEL_SYMBOL as STRING),
        |cast(FD_DUTY_COMPANY as STRING),
        |cast(FD_DUTY_COMPANY_TEXT as STRING),
        |cast(FD_RD_ERROR as STRING),
        |cast(FD_IS_SUSPEND as STRING),
        |cast(FD_RESP_PERSON as STRING),
        |cast(FD_RESP_CHEIF as STRING),
        |cast(FD_PLAN_TEST_PEOPLE as STRING),
        |cast(FD_TASK_ACCEPT as STRING),
        |cast(FD_PROVIDE_REASON as STRING),
        |cast(FD_HANDLE_PLAN as STRING),
        |cast(FD_ACCIDENT_J_OPINION as STRING),
        |cast(FD_ACCIDENT_P_OPINION as STRING),
        |cast(FD_OUTPUT_FACTOR as STRING),
        |cast(FD_OUTFLOW_CAUSE as STRING),
        |cast(FD_JUDGED_FINAL_OPINION as STRING),
        |cast(FD_FINAL_OPINION as STRING),
        |cast(FD_TEST_RESULT as STRING),
        |cast(FD_TEST_NG_REASON as STRING),
        |cast(FD_NPI_TEST_RESULT as STRING),
        |cast(FD_NPI_PROVIDE_REASON as STRING),
        |cast(FD_DQE_TEST_RESULT as STRING),
        |cast(FD_DQE_PROVIDE_REASON as STRING),
        |cast(FD_COMPLETE_NODE as STRING),
        |cast(FD_JUDGE_RH_ERROR as STRING),
        |cast(FD_ZHONGFUFASHENG as STRING),
        |cast(FD_EXCEPTION_TYPE as STRING),
        |cast(FD_MANAGER_ORTHER as STRING),
        |cast(FD_MANAGER_TEST as STRING),
        |cast(FD_MANAGER_PROVIDE as STRING),
        |cast(FD_MANAGER_JUDGE as STRING),
        |cast(FD_GUZHANGWEIYUANHUIYANSHOUCHE as STRING),
        |cast(FD_3916_TEXT as STRING),
        |cast(FD_ACCIDENT_TEST as STRING),
        |cast(FD_ACCIDENT_PROVIDE as STRING),
        |cast(FD_LEADER_TEST as STRING),
        |cast(FD_LEADER_PROVIDE as STRING),
        |cast(FD_JUDGE_SEDIMENT as STRING),
        |cast(FD_OUTFLOW_TIME as STRING),
        |cast(FD_SEDIMENT_TEST as STRING),
        |cast(FD_SEDIMENT_PROVIDE as STRING),
        |cast(FD_TOTAL_TEST_RESULT as STRING),
        |cast(FD_TOTAL_PROVIDE_REASON as STRING)
        |from tmp1
      """.stripMargin)

    targetDF.write.mode(SaveMode.Append)
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTER)
      .option("kudu.table", resultTable)
      .save()

  }

  def oa_km_review_main(spark:SparkSession):Unit = {
    var resultTable = "OA_KM_REVIEW_MAIN"

    oa_table(spark,"km_review_main","tmp1")
    kuduTable(spark,resultTable,"KM_REVIEW_MAIN")

    val targetDF = spark.sql(
      """
        |select
        |cast(FD_ID as STRING),
        |cast(SYNC_DATA_TO_CALENDAR_TIME as STRING),
        |cast(FD_LAST_MODIFIED_TIME as STRING),
        |cast(DOC_SUBJECT as STRING),
        |cast(FD_CAN_CIRCULARIZE as DOUBLE),
        |cast(FD_CURRENT_NUMBER as DOUBLE),
        |cast(FD_FEEDBACK_MODIFY as STRING),
        |cast(FD_FEEDBACK_EXECUTED as DOUBLE),
        |cast(FD_NUMBER as STRING),
        |cast(DOC_CREATOR_ID as STRING),
        |cast(DOC_CREATE_TIME as STRING),
        |cast(FD_DEPARTMENT_ID as STRING),
        |cast(DOC_PUBLISH_TIME as STRING),
        |cast(DOC_READ_COUNT as DOUBLE),
        |cast(EXTEND_FILE_PATH as STRING),
        |cast(FD_USE_FORM as DOUBLE),
        |cast(FD_DISABLE_MOBILE_FORM as DOUBLE),
        |cast(DOC_STATUS as STRING),
        |cast(AUTH_ATT_NODOWNLOAD as DOUBLE),
        |cast(AUTH_ATT_NOCOPY as DOUBLE),
        |cast(AUTH_ATT_NOPRINT as DOUBLE),
        |cast(AUTH_READER_FLAG as DOUBLE),
        |cast(FD_CHANGE_READER_FLAG as DOUBLE),
        |cast(FD_RBP_FLAG as DOUBLE),
        |cast(FD_CHANGE_ATT as DOUBLE),
        |cast(FD_MODEL_NAME as STRING),
        |cast(FD_MODEL_ID as STRING),
        |cast(FD_WORK_ID as STRING),
        |cast(FD_PHASE_ID as STRING),
        |cast(FD_TITLE_REGULATION as STRING),
        |cast(FD_TEMPLATE_ID as STRING),
        |cast(AUTH_AREA_ID as STRING)
        |from tmp1
      """.stripMargin)

    targetDF.write.mode(SaveMode.Append)
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTER)
      .option("kudu.table", resultTable)
      .save()

  }


  def oa_lbpm_audit_note(spark:SparkSession):Unit = {
    var resultTable = "OA_LBPM_AUDIT_NOTE"

    oa_table(spark,"lbpm_audit_note","tmp1")
    kuduTable(spark,resultTable,"LBPM_AUDIT_NOTE")

    val targetDF = spark.sql(
      """
        |select
        |cast(FD_ID as STRING),
        |cast(FD_FACT_NODE_ID as STRING),
        |cast(FD_FACT_NODE_NAME as STRING),
        |cast(FD_LANGS as STRING),
        |cast(FD_CURR_DEPT_OLD as STRING),
        |cast(FD_CURR_POST_OLD as STRING),
        |cast(FD_ACTION_KEY as STRING),
        |cast(FD_ACTION_NAME as STRING),
        |cast(FD_ACTION_INFO as STRING),
        |cast(FD_AUDIT_NOTE as STRING),
        |cast(FD_AUDIT_NOTE_FROM as STRING),
        |cast(FD_CREATE_TIME as STRING),
        |cast(FD_NODE_ID as STRING),
        |cast(FD_WORKITEM_ID as STRING),
        |cast(FD_NOTIFY_TYPE as STRING),
        |cast(FD_EXECUTION_ID as STRING),
        |cast(FD_PARENT_EXECUTION_ID as STRING),
        |cast(FD_PROCESS_ID as STRING),
        |cast(FD_HANDLER_ID as STRING),
        |cast(FD_BYACCR_ID as STRING),
        |cast(FD_EXPECTER_ID as STRING),
        |cast(FD_IDENTITY_ID as STRING),
        |cast(FD_ACCR_ID as STRING),
        |cast(FD_ACCR_TYPE as STRING),
        |cast(FD_COMMISSIONER_ID as STRING),
        |cast(FD_ADDITION_SIGNER_ID as STRING),
        |cast(FD_COST_TIME as DOUBLE),
        |cast(FD_EXT_NOTE as STRING),
        |cast(FD_PRIVATE_GROUP as STRING)
        |from tmp1
      """.stripMargin)

    targetDF.write.mode(SaveMode.Append)
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTER)
      .option("kudu.table", resultTable)
      .save()

  }

  def oa_lbpm_process(spark:SparkSession):Unit = {
    var resultTable = "OA_LBPM_PROCESS"

    oa_table(spark,"lbpm_process","tmp1")
    kuduTable(spark,resultTable,"LBPM_PROCESS")

    val targetDF = spark.sql(
      """
        |select
        |cast(FD_ID				 as STRING),
        |cast(FD_NAME				 as STRING),
        |cast(FD_TEMPLATE_ID		 as STRING),
        |cast(FD_STATUS			 as STRING),
        |cast(FD_MODEL_NAME		 as STRING),
        |cast(FD_MODEL_ID			 as STRING),
        |cast(FD_TEMPLATE_MODEL_ID as STRING),
        |cast(FD_KEY				 as STRING),
        |cast(FD_CREATE_TIME		 as STRING),
        |cast(FD_LAST_HANDLE_TIME	 as STRING),
        |cast(FD_ENDED_TIME		 as STRING),
        |cast(FD_CREATOR_ID		 as STRING),
        |cast(FD_LOAD_TYPE		 as DOUBLE),
        |cast(FD_COST_TIME		 as DOUBLE),
        |cast(FD_EFFICIENCY		 as DOUBLE),
        |cast(FD_IDENTITY_ID		 as STRING),
        |cast(FD_HIERARCHY_ID		 as STRING),
        |cast(FD_SOURCE_ID		 as STRING),
        |cast(FD_PARENT_NODE_FDID	 as STRING),
        |cast(FD_PARENT_NODEID	 as STRING),
        |cast(FD_SUB_STATUS		 as DOUBLE),
        |cast(FD_PARENTID			 as STRING),
        |cast(FD_CREATE_YEAR		 as STRING),
        |cast(FD_CREATE_MONTH		 as STRING)
        |from tmp1
      """.stripMargin)

    targetDF.write.mode(SaveMode.Append)
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTER)
      .option("kudu.table", resultTable)
      .save()

  }


}
