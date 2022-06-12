package renaissance.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import renaissance.bean.MetricBean;

import java.nio.charset.StandardCharsets;

public class MetricSink extends RichSinkFunction<MetricBean> {

    private static org.apache.hadoop.conf.Configuration configuration;
    private static Connection connection;
    private static StringBuffer stringBuffer;
    private static int count=0;
    private static BufferedMutator mutator;
    @Override
    public void open(Configuration parameters) throws Exception {
        configuration = HBaseConfiguration.create();

        configuration.set("hbase.master", "gateway1:60010");
        configuration.set("hbase.zookeeper.quorum", "master1,master2,utility1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        connection = ConnectionFactory.createConnection(configuration);


        BufferedMutatorParams mutatorParams = new BufferedMutatorParams(TableName.valueOf("metric"));

        mutatorParams.writeBufferSize(2*1024*1024);

        mutator = connection.getBufferedMutator(mutatorParams);
    }


    //{"id":972795,"name":"mem.used","hostname":"svr1002","value":9686,"timestamp":1655012826}

    @Override
    public void invoke(MetricBean metricBean, Context context) throws Exception {
        long unixtimestamp = 0;

       String rowkey= metricBean.getId()+ metricBean.getTimestamp();
        String value = metricBean.getValue();
        String hostName = metricBean.getHostName();
        String name = metricBean.getName();

        Put put = new Put(rowkey.getBytes());
        put.addColumn("f".getBytes(),"name".getBytes(),name.getBytes());
        put.addColumn("f".getBytes(),"value".getBytes(),value.getBytes());
        put.addColumn("f".getBytes(),"hostname".getBytes(),hostName.getBytes());

        mutator.mutate(put);
        if(count>=500){
            mutator.flush();
            count=0;
        }
        count++;


    }

    @Override
    public void close() throws Exception {
        mutator.close();
        connection.close();
    }
}
