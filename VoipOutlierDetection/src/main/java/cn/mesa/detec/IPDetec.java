package cn.mesa.detec;

import cn.mesa.bean.AbnormalRecord;
import cn.mesa.bean.VoipKnowledgeRecord;
import org.apache.spark.api.java.function.Function;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPDetec implements Function<VoipKnowledgeRecord, AbnormalRecord> {
	public AbnormalRecord call(VoipKnowledgeRecord record) {
        AbnormalRecord abnormalRecord = new AbnormalRecord();
        String service_name;
        if (isIP(record.service_name)) {
            if (record.service_name.indexOf(":") == -1) {
                service_name = record.service_name;
            }
            else {
                service_name = record.service_name.substring(0,record.service_name.indexOf(":"));
            }

            int numOfSame = 0;
            double sumOfAll = 0;
            for (int i = 0; i < record.ip_list.length; i ++) {
                if (record.ip_list[i].equals(service_name)) {
                    numOfSame += record.ip_list_freq[i];
                }
                sumOfAll += record.ip_list_freq[i];
            }
            //get exception
            if (numOfSame < sumOfAll * 0.1) {
                abnormalRecord = new AbnormalRecord(record.stat_time, record.service_name, 2);
                return abnormalRecord;
            }
            else {
                return abnormalRecord;
            }
        }
        return abnormalRecord;

    }


    public static boolean isIP(String addr)
    {
        if(addr.length() < 7 || addr.length() > 21 || "".equals(addr))
        {
            return false;
        }
        /**
         * * 判断IP格式和范围
         * */
        String rexp = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}" +
                "(:[1-9]\\d{0,3}|:[1-5]\\d{4}|:6[0-4]\\d{3}|:65[0-4]\\d{2}|:655[0-2]\\d|:6553[0-5])?";
        Pattern pat = Pattern.compile(rexp);
        Matcher mat = pat.matcher(addr);
        boolean ipAddress = mat.matches();
        return ipAddress;
    }


}
