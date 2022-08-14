package renaissance.main;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import renaissance.bean.MetricBean;
import renaissance.profunc.MetricBeanMap;
import renaissance.sink.MetricSink;

import java.util.Properties;

public class MetircFlowMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        env.setParallelism(1);

        kafkaProps.setProperty("bootstrap.servers", "worker1:9092,worker2:9092,worker3:9092");
        kafkaProps.setProperty("zookeeper.connect", "master1:2181,master2:2181,utility1:2181");
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("group.id", "metric_consumer_g");

        FlinkKafkaConsumer<String> kafkaCpuUse = new FlinkKafkaConsumer<>("mem.used", SimpleStringSchema.class.newInstance(), kafkaProps);
//        FlinkKafkaConsumer<String> kafkaCpuUsage = new FlinkKafkaConsumer<>("cpu.usage", SimpleStringSchema.class.newInstance(), kafkaProps);
        DataStreamSource<String> memUsedStream = env.addSource(kafkaCpuUse);
//        DataStreamSource<String> cpuUsageStream = env.addSource(kafkaCpuUsage);
//        DataStream<String> metricStream = memUsedStream.union(cpuUsageStream);

        SingleOutputStreamOperator<MetricBean> beanStream = memUsedStream.map(new MetricBeanMap());
        beanStream.addSink(new MetricSink());

        env.execute("MetricFlowStream");


    }
}
