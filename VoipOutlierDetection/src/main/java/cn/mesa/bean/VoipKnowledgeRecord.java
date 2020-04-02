package cn.mesa.bean;

import scala.Serializable;
import java.sql.Date;

public class VoipKnowledgeRecord implements Serializable {

	//only be used untill 4
	public String service_name;
    public Date stat_time;
    public String[] domain_invalid_list;
    public long[] domain_invalid_list_freq;
    public String[] ip_list;
    public long[] ip_list_freq;
    public String[] method_list;
    public long[] method_list_freq;
    public String[] res_stat_list;
    public long[] res_stat_list_freq;

    public VoipKnowledgeRecord(String service_name, Date stat_time,
                        String[] domain_invalid_list, long[] domain_invalid_list_freq,
                        String[] ip_list, long[] ip_list_freq,
                        String[] method_list, long[] method_list_freq, String[] res_stat_list, long[] res_stat_list_freq) {
        this.service_name = service_name;
        this.stat_time = stat_time;
        this.domain_invalid_list = domain_invalid_list;
        this.domain_invalid_list_freq = domain_invalid_list_freq;
        this.ip_list = ip_list;
        this.ip_list_freq = ip_list_freq;
        this.method_list = method_list;
        this.method_list_freq = method_list_freq;
        this.res_stat_list = res_stat_list;
        this.res_stat_list_freq = res_stat_list_freq;
    }

    public VoipKnowledgeRecord() {
        this.service_name = null;
        this.stat_time = null;
        this.domain_invalid_list = null;
        this.domain_invalid_list_freq = null;
        this.ip_list = null;
        this.ip_list_freq = null;
        this.method_list = null;
        this.method_list_freq = null;
        this.res_stat_list = null;
        this.res_stat_list_freq = null;
    }

}
