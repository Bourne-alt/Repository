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
//{"created_time":"2022-07-03T15:57:04Z","hostname":"svr1001","id":22800,"message":"free space warning (mb) for host disk","timenew":1660358547478,"status":"CLOSED","timestamp":1656845791000}

    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        JSONObject ele =  (JSONObject) JSONObject.parse(element);
        System.out.println("CpuUseagePeriodAssign:"+element);
        String timenew="";
        if(ele.containsKey("timenew")){
             timenew=ele.getString("timenew");

        }else {
            timenew=String.valueOf(System.currentTimeMillis());
        }
//        if(timenew.length()>10){
//            timenew=timenew.substring(0,timenew.length()-3);
//        }

        maxTs=Math.max(maxTs,Long.valueOf(timenew));
        //处理超长时间戳字段

        System.out.println("当前水位线："+maxTs);
        return maxTs;
    }


}
