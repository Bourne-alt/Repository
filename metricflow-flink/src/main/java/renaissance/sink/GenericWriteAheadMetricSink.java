package renaissance.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import renaissance.bean.MetricBean;

public class GenericWriteAheadMetricSink extends GenericWriteAheadSink<MetricBean> {

    private static org.apache.hadoop.conf.Configuration configuration;
    private static Connection connection;
    private static StringBuffer stringBuffer;
    private static int count=0;
    private static BufferedMutator mutator;

    public GenericWriteAheadMetricSink(CheckpointCommitter committer, TypeSerializer<MetricBean> serializer, String jobID) throws Exception {
        super(committer, serializer, jobID);
    }


    //{"id":972795,"name":"mem.used","hostname":"svr1002","value":9686,"timestamp":1655012826}



    @Override
    public void close() throws Exception {
        mutator.close();
        connection.close();
    }

    @Override
    protected boolean sendValues(Iterable<MetricBean> iterable, long l, long l1) throws Exception {
        return false;
    }

    @Override
    public void setKeyContextElement(StreamRecord<MetricBean> record) throws Exception {
        super.setKeyContextElement(record);
    }
}
