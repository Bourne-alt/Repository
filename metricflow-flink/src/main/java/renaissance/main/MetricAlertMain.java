package renaissance.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import renaissance.profunc.CpuusedAlertFunction;

import java.util.Properties;

public class MetricAlertMain {
    public static void main(String[] args) throws Exception {
int para=2;
        if(args.length>0){
            para=Integer.parseInt(args[0]);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(para);
        env.enableCheckpointing(10000l);


        //source:kafka
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", "worker1:9092,worker2:9092,worker3:9092");
        kafkaProp.setProperty("zookeeper.connect", "master1:2181,master2:2181,utility1:2181");

        kafkaProp.setProperty("auto.offset.reset", "earliest");
        kafkaProp.setProperty("group.id", "metric_consumer_g");

        FlinkKafkaConsumer<String> memUsed = new FlinkKafkaConsumer<>("cpu.usage", SimpleStringSchema.class.newInstance(), kafkaProp);

        DataStreamSource<String> memUsedStreaming = env.addSource(memUsed);
        KeyedStream<String, String> memUsedKeyedStream
                = memUsedStreaming.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                JSONObject jsonObject = new JSONObject(JSON.parseObject(s));
                return jsonObject.getString("hostname");

            }
        });

        SingleOutputStreamOperator<String> alertStream = memUsedKeyedStream.process(new CpuusedAlertFunction()).setParallelism(2);


        alertStream.print();

        env.execute("AlertStream");



    }
}
