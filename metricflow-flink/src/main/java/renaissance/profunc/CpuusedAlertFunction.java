package renaissance.profunc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CpuusedAlertFunction extends KeyedProcessFunction<String,String,String>  {

    int threshold=70;

    //存储最近一次键值分区状态value值
    private ValueState<String> lastValueState;
    //前一个注册计时器分区状态
    private ValueState<Long> lastTimerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册最近一次温度状态
        ValueStateDescriptor<String> lastValueDescriptor = new ValueStateDescriptor<>("lastValue", String.class,"1");
        lastValueState= getRuntimeContext().getState(lastValueDescriptor);
        //注册计时器时间
        ValueStateDescriptor<Long> lastTimerDescriptor = new ValueStateDescriptor<>("lastTimer", Long.class,0L);
        lastTimerState=getRuntimeContext().getState(lastTimerDescriptor);


    }

    @Override
    public void processElement(String s, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
        JSONObject jsonObject = JSON.parseObject(s);

        //将清理状态计时器设置为比记录时间戳晚一个小时
        Long currtimer = context.timestamp() + (3600 * 1000);
        Long timer = lastTimerState.value();

        //删除前一个计时器
        context.timerService().deleteEventTimeTimer(timer);
        context.timerService().registerEventTimeTimer(currtimer);
        //更新计时器时间戳状态
        lastTimerState.update(currtimer);

        String value = lastValueState.value();
        String currValue = jsonObject.getString("value");
       // {"hostname":"svr1002","name":"cpu.usage","id":1416798,"value":4,"timestamp":1656893846000}}

        int diffValue = Integer.parseInt(currValue) - Integer.parseInt(value);
        if(diffValue>threshold){
            jsonObject.put("lastvalue",value);
            jsonObject.put("diffValue",diffValue);
            collector.collect(jsonObject.toJSONString());
        }

        lastValueState.update(currValue);


    }


    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        lastValueState.clear();
        lastTimerState.clear();
    }
}
