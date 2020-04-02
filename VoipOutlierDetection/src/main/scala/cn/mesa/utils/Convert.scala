package cn.mesa.utils

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util
import java.util.Properties
import java.sql.Date

import cn.mesa.bean.VoipKnowledgeRecord


object Convert {


  def func(zipRecord: ((((String, (List[String], List[Long])), (String, (List[String], List[Long]))), (String, (List[String], List[Long]))), (String, (List[String], List[Long])))): VoipKnowledgeRecord = {
    var voipKnowledgeRecord = new VoipKnowledgeRecord
    //service_name
    voipKnowledgeRecord.service_name = zipRecord._2._1
    //println(voipKnowledgeRecord.service_name)
    voipKnowledgeRecord.domain_invalid_list = zipRecord._1._1._1._2._1.toArray

    voipKnowledgeRecord.domain_invalid_list_freq = zipRecord._1._1._1._2._2.toArray
    voipKnowledgeRecord.ip_list = zipRecord._2._2._1.toArray
    voipKnowledgeRecord.ip_list_freq = zipRecord._2._2._2.toArray
    voipKnowledgeRecord.method_list = zipRecord._1._1._2._2._1.toArray
    voipKnowledgeRecord.method_list_freq = zipRecord._1._1._2._2._2.toArray
    voipKnowledgeRecord.res_stat_list = zipRecord._1._2._2._1.toArray
    voipKnowledgeRecord.res_stat_list_freq = zipRecord._1._2._2._2.toArray


//    val timeString = PropUtils.getBeforeTime(PropUtils.upperDays, "yyyy-MM-dd")
//    println(timeString)
//    val timeUtil = new SimpleDateFormat("yyyy-MM-dd").parse(timeString)
    voipKnowledgeRecord.stat_time = PropUtils.recordTime

    //zipRecord
    voipKnowledgeRecord
  }

}

