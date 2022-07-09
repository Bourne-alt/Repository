package com.renaissance.main;

import com.alibaba.fastjson.JSONObject;
import com.renaissance.custormseri.KafkaSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MetricCollectMain {
    public static void main(String[] args) throws Exception {

        final OutputTag<String> memusedtag = new OutputTag<String>("mem.used") {
        };
        final OutputTag<String> cpuuseage = new OutputTag<String>("cpu.usage") {
        };

        String hostname = "master1";
        if (args.length > 0) {

            hostname = args[0];
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);


        DebeziumSourceFunction<String> metricCdc = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(3306)
                .password("Bdpp1234!")
                .username("root")
                .databaseList("bdp_metric")
                .deserializer(new KafkaSchema())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> metricCdcStream = env.addSource(metricCdc);


        //分流 sideoutput
        SingleOutputStreamOperator<String> mainStream = metricCdcStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String metric, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {

                JSONObject metricjson = (JSONObject) JSONObject.parse(metric);
                JSONObject after = (JSONObject) JSONObject.parse(metricjson.getString("after"));

                String name = null;
                if (after.containsKey("name")) {

                    name = (String) after.get("name");

                    if (name.equals("mem.used"))
                        context.output(memusedtag, after.toJSONString());
                    if (name.equals("cpu.usage"))
                        context.output(cpuuseage, after.toJSONString());

                }
                collector.collect(metric);
            }
        });

        DataStream<String> memusedStream = mainStream.getSideOutput(memusedtag);
        DataStream<String> cpuusageStream = mainStream.getSideOutput(cpuuseage);


        //sink
        FlinkKafkaProducer<String> memusedKafkaSink = new FlinkKafkaProducer<String>(
                "worker1:9092,worker2:9092,worker3:9092",
                "mem.used",
                new SimpleStringSchema()

        );
        FlinkKafkaProducer<String> cpuusageKafkaSink = new FlinkKafkaProducer<String>(
                "worker1:9092,worker2:9092,worker3:9092",
                "cpu.usage",
                new SimpleStringSchema()

        );


        memusedStream.addSink(memusedKafkaSink);
        cpuusageStream.addSink(cpuusageKafkaSink);

        memusedStream.print("memused:");
        cpuusageStream.print("cpuusage:");


        env.execute("metriccollect");


    }
}