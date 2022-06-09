package renaissance.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.ipc.Server;

import org.slf4j.LoggerFactory;

public class HbaseWriter extends RichSinkFunction {

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


        BufferedMutatorParams mutatorParams = new BufferedMutatorParams(TableName.valueOf("t1"));

        mutatorParams.writeBufferSize(2*1024*1024);

        mutator = connection.getBufferedMutator(mutatorParams);
    }

    @Override
    public void invoke(Object value, Context context) throws Exception {
        long unixtimestamp = 0;

    }

    @Override
    public void close() throws Exception {
        if(mutator!=null){
            mutator.close();
        }
        if(connection!=null){
            connection.close();
        }
    }
}
