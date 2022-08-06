package renaissance.profunc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import renaissance.bean.CpuUseageHighAndLowBean;

import java.util.Iterator;

public class CpuUsageHighAndLowAlertFunction extends ProcessAllWindowFunction<String, CpuUseageHighAndLowBean, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<String, CpuUseageHighAndLowBean, TimeWindow>.Context context, Iterable<String> elements, Collector<CpuUseageHighAndLowBean> out) throws Exception {
        System.out.println("窗口函数开始处理：");
        Iterator<String> eles = elements.iterator();

        CpuUseageHighAndLowBean bean = new CpuUseageHighAndLowBean();
        int id=1;
        long endTime = context.window().getEnd();
        long startTime = context.window().getStart();
        Integer high=0;
        Integer low = 0;
        String hostName="null";
        while(eles.hasNext()){
            // {"id":972795,"name":"mem.used","hostname":"svr1002","value":9686,"timestamp":1655012826}
            JSONObject message = (JSONObject) JSON.parse(eles.next());
            Integer value = message.getInteger("value");
            if(value>high) high=value;
            if(value<low)  low= value;
            hostName=message.getString("hostname");


        }
        bean.setHighUse(high);
        bean.setLowUse(low);
        bean.setId(id++);
        bean.setStartTime(String.valueOf(startTime));
        bean.setEndTime(String.valueOf(endTime));
        bean.setHostname(hostName);

        out.collect(bean);








    }
}
