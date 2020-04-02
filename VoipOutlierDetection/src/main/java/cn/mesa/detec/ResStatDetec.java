package cn.mesa.detec;

import cn.mesa.bean.AbnormalRecord;
import cn.mesa.bean.VoipKnowledgeRecord;
import org.apache.spark.api.java.function.Function;

public class ResStatDetec implements Function<VoipKnowledgeRecord, AbnormalRecord> {
	public AbnormalRecord call(VoipKnowledgeRecord record) {
        AbnormalRecord abnormalRecord = new AbnormalRecord();
//        for (int i = 0; i < record.res_stat_list.length; i ++){
//            System.out.println("444" + record.res_stat_list[i] + "hhhhh");
//        }
//        System.out.println("sfjhuisejhf88888888888888888888888888888888");
        if (record.res_stat_list.length == 1 && record.res_stat_list[0].length() <= 0) {
            System.out.println("sfjhuisejhf88888888888888888888888888888888");
            abnormalRecord = new AbnormalRecord(record.stat_time, record.service_name, 4);
            return abnormalRecord;
        }

        if (record.res_stat_list == null) {
            abnormalRecord = new AbnormalRecord(record.stat_time, record.service_name, 4);
            return abnormalRecord;
        }

        if (record.res_stat_list_freq == null) {
            abnormalRecord = new AbnormalRecord(record.stat_time, record.service_name, 4);
            return abnormalRecord;
        }
        long sumOfAll = 0;
        for (int i = 0; i < record.res_stat_list_freq.length; i ++) {
            sumOfAll += record.res_stat_list_freq[i];
        }
        if (sumOfAll == 0) {
            abnormalRecord = new AbnormalRecord(record.stat_time, record.service_name, 4);
            return abnormalRecord;
        }
        else {
            return abnormalRecord;
        }
    }
}
