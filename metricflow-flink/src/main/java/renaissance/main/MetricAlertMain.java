package renaissance.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import renaissance.bean.AlertBean;
import renaissance.bean.CpuUseageHighAndLowBean;
import renaissance.bean.MemUsedWithColorBean;
import renaissance.common.CpuUseagePeriodAssignTimestamp;
import renaissance.profunc.*;
import renaissance.sink.AlertMetricSink;
import renaissance.sink.CpuUsageHighAndLowSink;
import renaissance.sink.MemUsedWithColorSink;
import renaissance.source.ThresholdSource;

import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class MetricAlertMain {


    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger("renaissance.main.MetricAlertMain");
        int para = 1;
        if (args.length > 0) {
            para = Integer.parseInt(args[0]);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(para);
        env.enableCheckpointing(10000l);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //source:kafka
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", "worker1:9092,worker2:9092,worker3:9092");
        kafkaProp.setProperty("zookeeper.connect", "master1:2181,master2:2181,utility1:2181");

        kafkaProp.setProperty("auto.offset.reset", "latest");
        kafkaProp.setProperty("group.id", "metric_consumer_g");

        FlinkKafkaConsumer<String> cpuUsage = new FlinkKafkaConsumer<>("cpu.usage", SimpleStringSchema.class.newInstance(), kafkaProp);
        FlinkKafkaConsumer<String> memUsed = new FlinkKafkaConsumer<>("mem.used", SimpleStringSchema.class.newInstance(), kafkaProp);


        //维表流

        MapStateDescriptor<String, String> thresholdStateDesp = new MapStateDescriptor<>("ThresholdState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        BroadcastStream<String> thresholdBroadcastStream = env.addSource(
                new ThresholdSource()).broadcast(thresholdStateDesp);

        DataStreamSource<String> cpuUsageStreaming = env.addSource(cpuUsage);
        DataStreamSource<String> cpuUsageStreamingWindow = env.addSource(memUsed);
        DataStreamSource<String> memUseStreaming = env.addSource(memUsed);
        KeyedStream<String, String> cpuUsagesKeyedStream
                = cpuUsageStreaming.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                JSONObject jsonObject = new JSONObject(JSON.parseObject(s));
                return jsonObject.getString("hostname");

            }
        });

        //transaction

        SingleOutputStreamOperator<String> alertStream = cpuUsagesKeyedStream.process(new CpuusedAlertFunction());
        SingleOutputStreamOperator<AlertBean> alertBeanStream = alertStream.map(new AlertToBeanMap());

        /**
         * 窗口函数 统计5分钟窗口最大值最小值 结果写入mysql
         */

        SingleOutputStreamOperator<CpuUseageHighAndLowBean> memUseeHighAndLowString = cpuUsageStreamingWindow
                .assignTimestampsAndWatermarks(new CpuUseagePeriodAssignTimestamp())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new CpuUsageHighAndLowAlertFunction());
        /**
         * 窗口join  关联维表告警等级
         */

        SingleOutputStreamOperator<MemUsedWithColorBean> memusedWithColorStream = memUseStreaming.connect(thresholdBroadcastStream).process(new BroadcastProcessFunction<String, String, MemUsedWithColorBean>() {
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, MemUsedWithColorBean>.ReadOnlyContext ctx, Collector<MemUsedWithColorBean> out) throws Exception {

                ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(thresholdStateDesp);
                JSONObject ele = JSON.parseObject(value);

                MemUsedWithColorBean bean = new MemUsedWithColorBean();
                System.out.println("mapStatmemused:" + state.get(null));

                JSONObject thred = JSON.parseObject(state.get(null));
                int amberThreshold = Integer.parseInt(thred.getString("amber_threshold"));
                int redThreshold = Integer.parseInt(thred.getString("red_threshold"));
                String alertLev ;
                int eleVal = Integer.valueOf(ele.getString("value"));
                if (eleVal > amberThreshold && eleVal < redThreshold) {
                    alertLev = "amber";
                } else if (eleVal > redThreshold) {
                    alertLev = "red";
                } else {
                    alertLev = "green";
                }

                bean.setId(Integer.valueOf(ele.getString("id")));
                bean.setHostname(ele.getString("hostname"));
                bean.setMetricName(ele.getString("name"));
                bean.setValue(ele.getString("value"));
                bean.setAlertLev(alertLev);
                bean.setAmberThreshold(amberThreshold);
                bean.setRedThreshold(redThreshold);
                bean.setThrehCreateTime(thred.getString("creation_time"));
                bean.setThrehUpdateTime(thred.getString("update_time"));

                out.collect(bean);
            }


            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, MemUsedWithColorBean>.Context ctx, Collector<MemUsedWithColorBean> out) throws Exception {

                System.out.println("mapstate1:" + value);
                BroadcastState<String, String> state = ctx.getBroadcastState(thresholdStateDesp);
                state.clear();
                state.put(null, value.toString());
                System.out.println("statavalue:" + state.get(null));

            }
        });


        alertBeanStream.addSink(new AlertMetricSink());
        memUseeHighAndLowString.addSink(new CpuUsageHighAndLowSink());

        memusedWithColorStream.print("memusedWithColorStream");
        memusedWithColorStream.addSink(new MemUsedWithColorSink());
        env.execute("AlertStream");


    }
}
