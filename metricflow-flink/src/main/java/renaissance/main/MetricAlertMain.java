package renaissance.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import renaissance.bean.AlertBean;
import renaissance.bean.CpuUseageHighAndLowBean;
import renaissance.bean.MemUsedWithColorBean;
import renaissance.common.CpuUseagePeriodAssignTimestamp;
import renaissance.profunc.*;
import renaissance.sink.AlertMetricSink;
import renaissance.sink.CpuUsageHighAndLowSink;
import renaissance.sink.MemUsedWithColorSink;
import renaissance.source.ThresholdSource;

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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //source:kafka
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", "worker1:9092,worker2:9092,worker3:9092");
        kafkaProp.setProperty("zookeeper.connect", "master1:2181,master2:2181,utility1:2181");

        kafkaProp.setProperty("auto.offset.reset", "latest");
        kafkaProp.setProperty("group.id", "metric_consumer_g");

        FlinkKafkaConsumer<String> cpuUsage = new FlinkKafkaConsumer<>("cpu.usage", SimpleStringSchema.class.newInstance(), kafkaProp);
        FlinkKafkaConsumer<String> memUsed = new FlinkKafkaConsumer<>("mem.used", SimpleStringSchema.class.newInstance(), kafkaProp);

        //维表流

        DataStreamSource<String>  thresholdStream = env.addSource(new ThresholdSource());
        thresholdStream.print("threshold:");


        DataStreamSource<String> cpuUsageStreaming = env.addSource(cpuUsage);
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

        // {"hostname":"svr1002","name":"cpu.usage","id":1416798,"value":4,"timestamp":1656893846000}}

        SingleOutputStreamOperator<CpuUseageHighAndLowBean> memUseeHighAndLowString = memUseStreaming
                .assignTimestampsAndWatermarks(new CpuUseagePeriodAssignTimestamp())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new CpuUsageHighAndLowAlertFunction());


        memUseeHighAndLowString.print("memUseeHighAndLowString");

        /**
         * 窗口join  关联维表告警等级
         */

        DataStream<MemUsedWithColorBean> memUsedWithColorStream = memUseStreaming.join(thresholdStream).where(new KeySelector<String, Object>() {
            @Override
            public Object getKey(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                return jsonObject.getString("name");
            }
        }).equalTo(new KeySelector<String, Object>() {
            @Override
            public Object getKey(String s) throws Exception {
                return JSONObject.parseObject(s).getString("metric_name");
            }
        }).window(TumblingProcessingTimeWindows.of(Time.minutes(5))).apply(new Memuse5MinsAlertsFunction());

        //sink

        memUsedWithColorStream.addSink(new MemUsedWithColorSink());
        alertBeanStream.addSink(new AlertMetricSink());
        memUseeHighAndLowString.addSink(new CpuUsageHighAndLowSink());



        env.execute("AlertStream");


    }
}
