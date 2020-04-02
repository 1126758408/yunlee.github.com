package cn.mesa.utils


import java.io.FileInputStream
import java.sql.Date
import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import cn.mesa.bean.VoipKnowledgeRecord
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.sql.Column

//import cn.mesa.etl.{HiveIngestion, UnionHistory}
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object DFUtils {
  //private var props: Properties = new Properties
  private val logger: Logger = LoggerFactory.getLogger(DFUtils.getClass)

  def readByArayJoin(spark: SparkSession) = {

    val domainInvalidRdd = DFUtils.aggrClickhouseString("domain_invalid", spark)
    val reqIpRdd = DFUtils.aggrClickhouseIpPlatform(spark, "res")
    val methodRdd = DFUtils.aggrClickhouseString("method", spark)
    val resStatRdd = DFUtils.aggrClickhouseString("res_stat", spark)
    println("domainInvalidRdd.len = " + domainInvalidRdd.count())
    println(PropUtils.timeString)

    val resultRdd = domainInvalidRdd.zip(methodRdd).zip(resStatRdd).zip(reqIpRdd)

    val result: RDD[VoipKnowledgeRecord] = resultRdd.repartition(PropUtils.readKnowledgeBasePartition).mapPartitions(iter => {
      iter.map(Convert.func)
    })
    result.toJavaRDD()
  }


  //拼接字符串函数
  def concatString2Collect(Separator: String, fieldStr: String, alias: String): Column = {
    org.apache.spark.sql.functions.concat_ws(Separator, org.apache.spark.sql.functions.collect_list(fieldStr)).alias(alias)
  }

  def aggrClickhouseString(nested: String, spark: SparkSession): RDD[(String, (List[String], List[Long]))] = {
    val nested_List = nested + "." + nested + "_list"
    val nested_List_Freq = nested + "." + nested + "_list_freq"
    val sql = "(select service_name," + nested_List + " as " + nested + "_list," + nested_List_Freq + " " +
      "as " + nested + "_list_freq from " + PropUtils.knowledgeBaseTable + " " +
      "ARRAY JOIN " + nested + " where stat_time='" + PropUtils.timeString + "')"
    logger.warn(sql)
    val nest_Dataframe = spark.read.format("jdbc")
      .option("url", PropUtils.DBUrl)
      .option("dbtable", sql + " as a")
      .option("driver", PropUtils.DBDriver)
      .option("user", PropUtils.DBUser)
      .option("password", PropUtils.DBPassword)
      .load()
    nest_Dataframe.printSchema()

    val nestGroupByServiceDf = nest_Dataframe.repartition(PropUtils.readKnowledgeBasePartition).groupBy("service_name").agg(
      concatString2Collect("#&#", nested + "_list", nested + "_list"),
      concatString2Collect("#&#", nested + "_list_freq", nested + "_list_freq")
    )
    nestGroupByServiceDf.printSchema()
    val value: RDD[(String, (List[String], List[Long]))] = nestGroupByServiceDf.rdd.mapPartitions(iter => {
      iter.map(row => {
        val service = row.getAs[String]("service_name")
        val nest_List = row.getAs[String](nested + "_list").split("#&#")
        val nest_List_Freq = row.getAs[String](nested + "_list_freq").split("#&#").map(_.toLong)
        val new_nested: Map[String, Long] = nest_List.zip(nest_List_Freq).groupBy(_._1).map(
          tup => {
            (tup._1, tup._2.aggregate(0L)(_ + _._2, _ + _))
          }
        )
        (service, (new_nested.keySet.toList, new_nested.values.toList))
      })
    })
    value
  }

  def aggrClickhouseIpPlatform(spark: SparkSession,ipNested:String): RDD[(String, (List[String], List[Long]))] = {

    val nestIpList=ipNested+"_ip_platform."+ ipNested +"_ip_list"
    val nestIpListFreq=ipNested+"_ip_platform."+ ipNested +"_ip_list_freq"
    val sql= "(select service_name," + nestIpList+" as ip_list,"+ nestIpListFreq+" as ip_list_freq from " +
      PropUtils.knowledgeBaseTable + " " + "ARRAY JOIN " + ipNested+"_ip_platform" +
      " where stat_time='" + PropUtils.timeString + "')"
    logger.warn(sql)
    val ip_Platform_Dataframe = spark.read.format("jdbc")
      .option("url", PropUtils.DBUrl)
      .option("dbtable", sql + " as a")
      .option("driver", PropUtils.DBDriver)
      .option("user", PropUtils.DBUser)
      .option("password", PropUtils.DBPassword)
      .load()
    ip_Platform_Dataframe.printSchema()

    val ipPlatformGroupByServiceDf: DataFrame = ip_Platform_Dataframe.repartition(PropUtils.readKnowledgeBasePartition).groupBy("service_name").agg(
      concatString2Collect(",", "ip_list", "ip_list"),
      concatString2Collect(",", "ip_list_freq", "ip_list_freq")
    )
    ipPlatformGroupByServiceDf.printSchema()
    val IpPlatformAggr: RDD[(String, (List[String], List[Long]))] = ipPlatformGroupByServiceDf.rdd.mapPartitions(iter => {
      iter.map(row => {
        val service = row.getAs[String]("service_name")
        val ip_List = row.getAs[String]("ip_list").split(",")
        val ip_List_Freq = row.getAs[String]("ip_list_freq").split(",").map(_.toLong)

        val result: Map[String, Long] = ip_List.zip(ip_List_Freq).groupBy(_._1).map(tup => {
          (tup._1, tup._2.aggregate(0L)(_ + _._2, _ + _))
        })
        (service, (result.keySet.toList, result.values.toList))

      })
    })
    IpPlatformAggr
  }

}
