package cn.mesa.common;

import cn.mesa.bean.VoipKnowledgeRecord;
import cn.mesa.utils.Convert;
import cn.mesa.utils.DFUtils;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class ReadClickhouse {
//
//    public static List<VoipKnowledgeRecord> voipKnowledgeRecord = new ArrayList<VoipKnowledgeRecord>();
//    private static Properties props = new Properties();
//
//    public static JavaRDD<VoipKnowledgeRecord> getResults(SparkSession spark) throws SQLException, IOException {
//        props.load(ReadClickhouse.class.getClassLoader().getResourceAsStream("application.properties"));
//
//        String KnowledgeBaseTable = props.getProperty("KnowledgeBaseTable");
//        String RecordTime = props.getProperty("RecordTime");
//        String DBurl = props.getProperty("db.url");
//        String DBdriver = props.getProperty("drivers");
//        String DBuser = props.getProperty("mdb.user");
//        String DBpassword = props.getProperty("mdb.password");
//        Integer ReadKnowledgeBasePartition = Integer.parseInt(props.getProperty("ReadKnowledgeBasePartition"));
//        System.out.println("RecordTime = " + RecordTime);
//
//
//
//        DbConnection ins = DbConnection.getInstance();
//        DruidPooledConnection conn = ins.getConnection();
//        Statement statement = conn.createStatement();
//        ResultSet results = statement.executeQuery(
//                "select " +
//                "stat_time," +
//                "service_name," +
//                "domain_invalid.domain_invalid_list,domain_invalid.domain_invalid_list_freq," +
//                "res_ip_platform.res_ip_list,res_ip_platform.res_ip_list_freq," +
//                "method.method_list,method.method_list_freq," +
//                "res_stat.res_stat_list_freq " +
//                "from " + KnowledgeBaseTable +
//                        " WHERE stat_time=" + RecordTime + ";");
//        return results;
//    }
//
//
//    public static List<VoipKnowledgeRecord> getRecordList(ResultSet result) throws SQLException {
//        while (result.next()) {
//            String service_name = result.getString("service_name");
//            Date stat_time = result.getDate("stat_time");
//
//            String[] domain_invalid_list = (String[]) result.getArray("domain_invalid.domain_invalid_list").getArray();
//            long[] domain_invalid_list_freq = (long[]) result.getArray("domain_invalid.domain_invalid_list_freq").getArray();
//
//            String[] ip_list = (String[]) result.getArray("res_ip_platform.res_ip_list").getArray();
//            long[] ip_list_freq = (long[]) result.getArray("res_ip_platform.res_ip_list_freq").getArray();
//
//            String[] method_list = (String[]) result.getArray("method.method_list").getArray();
//            long[] method_list_freq = (long[]) result.getArray("method.method_list_freq").getArray();
//
//            String[] res_stat_list = (String[]) result.getArray("res_stat.res_stat_list").getArray();
//            long[] res_stat_list_freq = (long[]) result.getArray("res_stat.res_stat_list_freq").getArray();
//
//            voipKnowledgeRecord.add(new VoipKnowledgeRecord(service_name, stat_time,
//                    domain_invalid_list, domain_invalid_list_freq,
//                    ip_list, ip_list_freq,
//                    method_list, method_list_freq,
//                    res_stat_list, res_stat_list_freq));
//        }
//
//        System.out.println("finish read!");
//        return voipKnowledgeRecord;
//    }


    

}
