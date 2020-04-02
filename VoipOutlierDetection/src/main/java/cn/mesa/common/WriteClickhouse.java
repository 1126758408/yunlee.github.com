package cn.mesa.common;

import cn.mesa.bean.AbnormalRecord;
import cn.mesa.bean.VoipKnowledgeRecord;
import cn.mesa.utils.PropUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.serializer.JavaSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteClickhouse extends JavaSerializer {
    //private static Properties props = new Properties();

    public static void writeAbnormal(JavaRDD<AbnormalRecord> abnormalRecordJavaRDD) throws IOException {
        System.out.println("The abnormalTbale is: " + PropUtils.abnormalServiceTable());
        //System.out.println(PropUtils.batchNum());
        System.out.println("The size of abnormal record is: " + abnormalRecordJavaRDD.count());

        abnormalRecordJavaRDD.coalesce(10).foreachPartition(abnormalRecordIterator -> {
            AtomicInteger numOfRecord = new AtomicInteger();
            DbConnection dbConnection = DbConnection.getInstance();
            Connection connection = dbConnection.getConnection();
            String sql = "INSERT INTO " + PropUtils.abnormalServiceTable() +
                    " " + "VALUES(?,?,?)";
            System.out.println(sql);
            PreparedStatement pstm = connection.prepareStatement(sql);
            abnormalRecordIterator.forEachRemaining(abnormalRecord -> {

                if (abnormalRecord.service_name != null) {

                    try {
                        pstm.setDate(1, abnormalRecord.found_time);
                        pstm.setString(2, abnormalRecord.service_name);
                        pstm.setInt(3, abnormalRecord.abnormal_type);

                        numOfRecord.getAndIncrement();
                        pstm.addBatch();

                        if (numOfRecord.get() == PropUtils.batchNum()) {
                            pstm.executeBatch();
                            connection.commit();
                            numOfRecord.set(0);
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

            });
            pstm.executeBatch();
            connection.commit();
            numOfRecord.set(0);
        });
    }
}
