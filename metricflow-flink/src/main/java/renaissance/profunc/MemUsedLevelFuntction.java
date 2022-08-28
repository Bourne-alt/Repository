package renaissance.profunc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import renaissance.bean.MemUsedWithColorBean;

public class MemUsedLevelFuntction extends ProcessFunction<String, MemUsedWithColorBean> {
    int id=0;
    @Override
    public void processElement(String ele, ProcessFunction<String, MemUsedWithColorBean>.Context ctx, Collector<MemUsedWithColorBean> out) throws Exception {
        JSONObject elements = JSONObject.parseObject(ele);
        MemUsedWithColorBean bean = new MemUsedWithColorBean();
        bean.setId(id++);



    }
}
