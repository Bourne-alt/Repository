package com.renaissance.main;

import com.renaissance.custormseri.KafkaSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MetricCollectMain {
    public static void main(String[] args) throws Exception {

        String hostname = "master1";
        if (args.length > 0) {

            hostname = args[0];
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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



        metricCdcStream.print();

        env.execute();


    }
}