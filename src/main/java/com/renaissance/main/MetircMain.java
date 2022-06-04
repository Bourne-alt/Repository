package com.renaissance.main;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class MetircMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        env.setParallelism(1);
//ecs远程kafka
        kafkaProps.setProperty("bootstrap.servers", "47.104.89.120:9092,47.104.136.80:9092,47.104.133.128:9092");
        kafkaProps.setProperty("zookeeper.connect", "118.190.209.213:2181,47.104.22.125:2181,118.190.207.81:2181");
        //local
//        kafkaProps.setProperty("zookeeper.connect", "10.27.189.244:2181,10.27.189.236:2181,10.27.189.227:2181");
//        kafkaProps.setProperty("bootstrap.servers", "10.27.189.238:9092,10.27.189.239:9092,10.27.189.240:9092 ");
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("group.id", "metric_consumer_g");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("mem.used", SimpleStringSchema.class.newInstance(), kafkaProps);
        DataStreamSource<String> metricStream = env.addSource(kafkaConsumer);

        metricStream.print();

        env.execute("metricStream");


    }
}
