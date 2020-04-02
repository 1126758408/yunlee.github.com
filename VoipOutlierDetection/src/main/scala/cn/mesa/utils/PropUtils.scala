package cn.mesa.utils

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.{Config, ConfigFactory}

object PropUtils {
  private lazy val config: Config = ConfigFactory.load()
  val appName = config.getString("AppName")
  val knowledgeBaseTable = config.getString("KnowledgeBaseTable")
  val abnormalServiceTable = config.getString("AbnormalServiceTable")
  val batchNum = config.getInt("batch_num")
  //val timeString = config.getString("RecordTime")
  //val timeUtil = new SimpleDateFormat("yyyy-MM-dd").parse(timeString)
  //val recordTime = new Date(timeUtil.getTime)
  val DBUrl = config.getString("db.url")
  val writeDBUrl = config.getString("writedb.url")
  val DBDriver = config.getString("drivers")
  val DBUser = config.getString("mdb.user")
  val DBPassword = config.getString("mdb.password")
  val readKnowledgeBasePartition = config.getInt("ReadKnowledgeBasePartition")
  val initial = config.getInt("initialsize")
  val minidle = config.getInt("minidle")
  val maxactive = config.getInt("maxactive")
  val upperDays = config.getInt("read.history.clickhouse.upper.days")

  def getBeforeTime(int: Integer, dateFormatStr: String): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat(dateFormatStr)
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, int)
    var time: String = dateFormat.format(cal.getTime)
    time
  }
  val timeString = getBeforeTime(upperDays, "yyyy-MM-dd")
  //println(timeString)
  val timeUtil = new SimpleDateFormat("yyyy-MM-dd").parse(timeString)
  val recordTime = new Date(timeUtil.getTime)

}
