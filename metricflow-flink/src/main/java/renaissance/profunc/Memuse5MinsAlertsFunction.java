package renaissance.profunc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.JoinFunction;

public class Memuse5MinsAlertsFunction implements JoinFunction<String, String, String> {
    @Override
    public String join(String memused, String threshold) throws Exception {
        JSONObject memusedJson = (JSONObject) JSON.parse(memused);
        JSONObject thresholdJson = (JSONObject)JSONObject.parse(threshold);

        // {"id":972795,"name":"mem.used","hostname":"svr1002","value":9686,"timestamp":1655012826}
//        server_id	amber_threshold	creation_time	red_threshold	update_time	metric_name
//        2	5120	2018-09-01 00:00:00	5760	2018-09-01 00:00:00	mem.used        memusedJson.put()
        int amberThreshold=Integer.valueOf(thresholdJson.getString("amber_threshold"));
        int redThreshold=Integer.valueOf(thresholdJson.getString("red_threshold"));
        String alertLev="";
        int value=Integer.valueOf(memusedJson.getString("value"));
        if(value>amberThreshold&&value<redThreshold){
            alertLev="amber";
        }else if(value>redThreshold){
            alertLev="red";
        }else {
            alertLev="green";
        }
        memusedJson.put("alertLev",alertLev);
        memusedJson.put("threhUpdateTime",thresholdJson.getString("update_time"));
        memusedJson.put("threhCreateTime",thresholdJson.getString("creation_time"));
        memusedJson.put("amberThreshold",amberThreshold);
        memusedJson.put("redThreshold",redThreshold);
        memusedJson.put("metricName",thresholdJson.getString("metric_name"));

        return memusedJson.toJSONString();
    }
}
