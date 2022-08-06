package renaissance.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import renaissance.bean.AlertBean;
import renaissance.bean.CpuUseageHighAndLowBean;
import renaissance.common.CpuUseagePeriodAssignTimestamp;
import renaissance.profunc.AlertToBeanMap;
import renaissance.profunc.CpuUsageHighAndLowAlertFunction;
import renaissance.profunc.CpuusedAlertFunction;
import renaissance.sink.AlertMetricSink;
import renaissance.sink.CpuUsageHighAndLowSink;

import java.util.Properties;
import java.util.logging.Logger;

public class MetricAlertMain {

    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger("renaissance.main.MetricAlertMain");
        int para = 2;
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

        kafkaProp.setProperty("auto.offset.reset", "earliest");
        kafkaProp.setProperty("group.id", "metric_consumer_g");

        FlinkKafkaConsumer<String> cpuUsage = new FlinkKafkaConsumer<>("cpu.usage", SimpleStringSchema.class.newInstance(), kafkaProp);

        logger.info("source");
        DataStreamSource<String> cpuUsageStreaming = env.addSource(cpuUsage);
        KeyedStream<String, String> cpuUsagesKeyedStream
                = cpuUsageStreaming.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                JSONObject jsonObject = new JSONObject(JSON.parseObject(s));
                return jsonObject.getString("hostname");

            }
        });

        //transaction
        logger.info("transaction");
        SingleOutputStreamOperator<String> alertStream = cpuUsagesKeyedStream.process(new CpuusedAlertFunction());
        SingleOutputStreamOperator<AlertBean> alertBeanStream = alertStream.map(new AlertToBeanMap());

        /**
         * 窗口函数 统计5分钟窗口最大值最小值 结果写入mysql
         */

        // {"hostname":"svr1002","name":"cpu.usage","id":1416798,"value":4,"timestamp":1656893846000}}

        SingleOutputStreamOperator<CpuUseageHighAndLowBean> cpuUsageHighAndLowString = cpuUsageStreaming.assignTimestampsAndWatermarks(new CpuUseagePeriodAssignTimestamp())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new CpuUsageHighAndLowAlertFunction());


        cpuUsageHighAndLowString.print("cpuUsageHighAndLowString");

        //sink

        alertBeanStream.addSink(new AlertMetricSink());

        cpuUsageHighAndLowString.addSink(new CpuUsageHighAndLowSink());
        alertStream.print();

        env.execute("AlertStream");


    }
}
