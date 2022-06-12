package renaissance.profunc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import renaissance.bean.MetricBean;



public class MetricBeanMap implements MapFunction<String, MetricBean> {

    @Override
    public MetricBean map(String s) throws Exception {
        //{"id":972795,"name":"mem.used","hostname":"svr1002","value":9686,"timestamp":1655012826}
        MetricBean metricBean = new MetricBean();
        JSONObject metric = (JSONObject) JSON.parse(s);
        metricBean.setId(metric.getString("id"));
        metricBean.setHostName(metric.getString("hostname"));
        metricBean.setName(metric.getString("name"));
        metricBean.setValue(metric.getString("value"));
        metricBean.setTimestamp(metric.getString("timestamp"));



        return metricBean;
    }
}
