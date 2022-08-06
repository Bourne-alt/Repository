package renaissance.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class CpuUseagePeriodAssignTimestamp implements AssignerWithPeriodicWatermarks<String> {

    //1分钟容忍时间
    long bound=60*1000;
    long maxTs=0l;
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
       return new Watermark(maxTs-bound);
    }

    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        JSONObject ele =  (JSONObject) JSONObject.parse(element);
        maxTs=Math.max(maxTs,Long.valueOf(ele.getString("timestamp")));
        return maxTs;
    }
}
