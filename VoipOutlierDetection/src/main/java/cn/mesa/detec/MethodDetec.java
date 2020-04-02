package cn.mesa.detec;

import cn.mesa.bean.AbnormalRecord;
import cn.mesa.bean.VoipKnowledgeRecord;
import org.apache.spark.api.java.function.Function;

public class MethodDetec implements Function<VoipKnowledgeRecord, AbnormalRecord> {
    public AbnormalRecord call(VoipKnowledgeRecord record) {
        AbnormalRecord abnormalRecord = new AbnormalRecord();
        long numOfOptions = 0;
        long sumOfAll = 0;
        for (int i = 0; i < record.method_list.length; i ++) {
            if (record.method_list[i].equals("OPTIONS")) {
                numOfOptions += record.method_list_freq[i];
            }
            sumOfAll += record.method_list_freq[i];
        }
        if (numOfOptions == sumOfAll) {
            abnormalRecord = new AbnormalRecord(record.stat_time, record.service_name, 3);
            return abnormalRecord;
        }
        else {
            return abnormalRecord;
        }
    }
}
