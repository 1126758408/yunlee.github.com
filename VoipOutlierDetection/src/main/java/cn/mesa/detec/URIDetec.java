package cn.mesa.detec;

import cn.mesa.bean.AbnormalRecord;
import cn.mesa.bean.VoipKnowledgeRecord;
import org.apache.spark.api.java.function.Function;


public class URIDetec implements Function<VoipKnowledgeRecord, AbnormalRecord> {
	public AbnormalRecord call(VoipKnowledgeRecord record) {
        AbnormalRecord abnormalRecord = new AbnormalRecord();

        long numOf1 = 0;
        double sumOfAll = 0;
        //System.out.println("record.service_name = " + record.service_name);
        for (int i = 0; i < record.domain_invalid_list.length; i ++) {
            if (record.domain_invalid_list[i].equals("1")) {
                numOf1 += record.domain_invalid_list_freq[i];
            }
            sumOfAll += record.domain_invalid_list_freq[i];
        }
        if (numOf1 > sumOfAll * 0.9) {
            System.out.println("111111111111111111111111111");
            abnormalRecord = new AbnormalRecord(record.stat_time, record.service_name, 1);
            return abnormalRecord;
        }
        else {
            return abnormalRecord;
        }
    }
}
