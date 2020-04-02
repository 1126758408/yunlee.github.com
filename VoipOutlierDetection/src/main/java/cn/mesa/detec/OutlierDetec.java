package cn.mesa.detec;

import cn.mesa.bean.AbnormalRecord;
import cn.mesa.bean.VoipKnowledgeRecord;
import cn.mesa.common.WriteClickhouse;
import cn.mesa.utils.DFUtils;
import cn.mesa.utils.PropUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Properties;

public class OutlierDetec {
    //private static Properties props = new Properties();

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession.builder().appName(PropUtils.appName()).getOrCreate();
        JavaRDD<VoipKnowledgeRecord> VoipRecordRDD = DFUtils.readByArayJoin(spark);
        System.out.println("read finish");

        //true is abnormal
        JavaRDD<AbnormalRecord> URIAbnormal = VoipRecordRDD.map(new URIDetec());
        JavaRDD<AbnormalRecord> IPAbnormal = VoipRecordRDD.map(new IPDetec());
        JavaRDD<AbnormalRecord> MethodAbnormal = VoipRecordRDD.map(new MethodDetec());
        JavaRDD<AbnormalRecord> ResStatAbnormal = VoipRecordRDD.map(new ResStatDetec());

        //rdd-hebing;
        JavaRDD<AbnormalRecord> abnormalRecordJavaRDD = URIAbnormal.union(IPAbnormal);
        abnormalRecordJavaRDD = abnormalRecordJavaRDD.union(MethodAbnormal);
        abnormalRecordJavaRDD = abnormalRecordJavaRDD.union(ResStatAbnormal);

        WriteClickhouse.writeAbnormal(abnormalRecordJavaRDD);


    }


}
