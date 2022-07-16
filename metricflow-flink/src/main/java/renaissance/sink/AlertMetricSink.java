package renaissance.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import renaissance.bean.AlertBean;

import java.sql.Connection;

public class AlertMetricSink extends TwoPhaseCommitSinkFunction<AlertBean, Connection,Void> {

    public AlertMetricSink(TypeSerializer<Connection> transactionSerializer, TypeSerializer<Void> contextSerializer) {
        super(transactionSerializer, contextSerializer);
    }

    @Override
    protected void invoke(Connection connection, AlertBean alertBean, Context context) throws Exception {

    }

    @Override
    protected Connection beginTransaction() throws Exception {
        return null;
    }

    @Override
    protected void preCommit(Connection connection) throws Exception {

    }

    @Override
    protected void commit(Connection connection) {

    }

    @Override
    protected void abort(Connection connection) {

    }
}
