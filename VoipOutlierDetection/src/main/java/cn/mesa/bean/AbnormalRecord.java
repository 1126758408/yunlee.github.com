package cn.mesa.bean;

import scala.Serializable;
import java.sql.Date;

public class AbnormalRecord implements Serializable {
	
	public Date found_time;
    public String service_name;
    public Integer abnormal_type;

    public AbnormalRecord() {
        this.found_time = null;
        this.service_name = null;
        this.abnormal_type = null;
    }

    public AbnormalRecord(Date found_time, String service_name, Integer abnormal_type) {
        this.found_time = found_time;
        this.service_name = service_name;
        this.abnormal_type = abnormal_type;
    }
}
