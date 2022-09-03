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
        //告警入表id

        //维表流

        MapStateDescriptor<String, String> thresholdStateDesp = new MapStateDescriptor<>("ThresholdState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        BroadcastStream<String> thresholdBroadcastStream = env.addSource(
                new ThresholdSource()).broadcast(thresholdStateDesp);

        DataStreamSource<String> cpuUsageStreaming = env.addSource(cpuUsage);
        DataStreamSource<String> memUseStreaming = env.addSource(memUsed);
        memUseStreaming.print("memusedString");
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

        SingleOutputStreamOperator<MemUsedWithColorBean> memusedWithColorStream = memUseStreaming.connect(thresholdBroadcastStream).process(new BroadcastProcessFunction<String, String, MemUsedWithColorBean>() {
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, MemUsedWithColorBean>.ReadOnlyContext ctx, Collector<MemUsedWithColorBean> out) throws Exception {

                ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(thresholdStateDesp);
                JSONObject ele = JSON.parseObject(value);
                String key = ele.getString("name");
                System.out.println("ele:"+ele);
                MemUsedWithColorBean bean = new MemUsedWithColorBean();
                //mapstate:{"creation_time":"2018-09-01 00:00:00","update_time":"2018-09-01 00:00:00"
                // ,"amber_threshold":"80","metric_name":"cpu.usage","red_threshold":"90","server_id":"2"}
                System.out.println("mapStatmemused:"+state.get(key));
                if(state.contains(key)){
                    JSONObject thred = JSON.parseObject(state.get(key));
                    int amberThreshold = Integer.valueOf(thred.getString("amber_threshold"));
                    int redThreshold = Integer.valueOf(thred.getString("red_threshold"));
                    String alertLev = "";
                    int eleVal = Integer.valueOf(ele.getString("value"));
                    if (eleVal > amberThreshold && eleVal < redThreshold) {
                        alertLev = "amber";
                    } else if (eleVal > redThreshold) {
                        alertLev = "red";
                    } else {
                        alertLev = "green";
                    }
                      // {"id":972795,"name":"mem.used","hostname":"svr1002","value":9686,"timestamp":1655012826}

                    bean.setId(Integer.valueOf(ele.getString("id")));
                    bean.setHostname(ele.getString("hostname"));
                    bean.setMetricName(key);
                    bean.setValue(ele.getString("value"));
                    bean.setAlertLev(alertLev);
                    bean.setAmberThreshold(amberThreshold);
                    bean.setRedThreshold(redThreshold);
                    bean.setThrehCreateTime(thred.getString("creation_time"));
                    bean.setThrehUpdateTime(thred.getString("update_time"));

                    out.collect(bean);
                }

                // {"id":972795,"name":"mem.used","hostname":"svr1002","value":9686,"timestamp":1655012826}
//        server_id	amber_threshold	creation_time	red_threshold	update_time	metric_name
//        2	5120	2018-09-01 00:00:00	5760	2018-09-01 00:00:00	mem.used        memusedJson.put()


            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, MemUsedWithColorBean>.Context ctx, Collector<MemUsedWithColorBean> out) throws Exception {
   //mapstate:{"creation_time":"2018-09-01 00:00:00","update_time":"2018-09-01 00:00:00"
   // ,"amber_threshold":"80","metric_name":"cpu.usage","red_threshold":"90","server_id":"2"}
                System.out.println("mapstate1:" + value);
                JSONObject ele = JSON.parseObject(value);
                String key="";
                if(ele.containsKey("metric_name")){
                    key=ele.getString("metric_name");
                }

                BroadcastState<String, String> state = ctx.getBroadcastState(thresholdStateDesp);
                if(!state.contains(key)){
                    state.put(key,ele.toString());
                }
            }
        });


        alertBeanStream.addSink(new AlertMetricSink());
        memUseeHighAndLowString.addSink(new CpuUsageHighAndLowSink());



        memusedWithColorStream.print("memusedWithColorStream");
        memusedWithColorStream.addSink(new MemUsedWithColorSink());
        env.execute("AlertStream");


    }
}
