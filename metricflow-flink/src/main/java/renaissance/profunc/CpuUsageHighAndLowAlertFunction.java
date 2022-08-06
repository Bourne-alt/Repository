package renaissance.profunc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class CpuUsageHighAndLowAlertFunction extends ProcessAllWindowFunction<String,String, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
        System.out.println("窗口函数开始处理：");
        Iterator<String> eles = elements.iterator();

        long endTime = context.window().getEnd();
        long startTime = context.window().getStart();
        Integer high=0;
        Integer low = 0;
        while(eles.hasNext()){
            // {"id":972795,"name":"mem.used","hostname":"svr1002","value":9686,"timestamp":1655012826}
            JSONObject message = (JSONObject) JSON.parse(eles.next());
            Integer value = message.getInteger("value");
            if(value>high) high=value;
            if(value<low)  low= value;

        }
        JSONObject results = new JSONObject();
        results.put("endTime",endTime);
        results.put("startTime",startTime);
        results.put("highvalue",high);
        results.put("lowvalue",low);

        out.collect(results.toJSONString());


    }
}
