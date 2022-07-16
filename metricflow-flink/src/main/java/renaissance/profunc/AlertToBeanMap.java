package renaissance.profunc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import renaissance.bean.AlertBean;

public class AlertToBeanMap implements MapFunction<String, AlertBean> {
    @Override
    public AlertBean map(String s) throws Exception {
        JSONObject jsonObject = (JSONObject) JSONObject.parse(s);
        //{"hostname":"svr1002","lastvalue":"10","name":"cpu.usage","id":1734528,"value":92,"timestamp":1657943471}
        AlertBean bean = new AlertBean();
        String color;
        String diffvalue = jsonObject.getString("diffValue");
        if(Integer.parseInt(diffvalue)<80) {
            color="amber";
        }else {
            color="red";
        }
        bean.setId(jsonObject.getString("id"));
        bean.setTimestamp(jsonObject.getString("timestamp"));
        bean.setHostname(jsonObject.getString("hostname"));
        bean.setValue(jsonObject.getString("value"));
        bean.setLastvalue(jsonObject.getString("lastvalue"));
        bean.setDiffValue(diffvalue);
        bean.setColor(color);


        return bean;

    }
}
