package renaissance.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Test;

import javax.annotation.Nullable;

public class CpuUseagePeriodAssignTimestamp implements AssignerWithPeriodicWatermarks<String> {

    //1分钟容忍时间
    // {"hostname":"svr1002","name":"cpu.usage","id":1416798,"value":4,"timestamp":1656893846000}}
    long bound=30*1000;
    long maxTs=1660055655l;
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
       return new Watermark(maxTs-bound);
    }


    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        JSONObject ele =  (JSONObject) JSONObject.parse(element);
        String timestamp=ele.getString("timestamp");
        if(timestamp.length()>10){
            timestamp=timestamp.substring(0,timestamp.length()-3);
        }

        maxTs=Math.max(maxTs,Long.valueOf(timestamp));
        //处理超长时间戳字段

        System.out.println("当前水位线："+maxTs);
        return maxTs;
    }


}
