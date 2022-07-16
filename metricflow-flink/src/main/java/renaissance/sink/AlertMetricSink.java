package renaissance.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import renaissance.bean.AlertBean;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AlertMetricSink extends TwoPhaseCommitSinkFunction<AlertBean, Connection, Void> {
    public Connection conn = null;
    public PreparedStatement ps = null;
    int batch=0;

    public AlertMetricSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);

    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = DriverManager.getConnection(
                "jdbc:mysql://master1:3306/bdp_metric?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true"
                , "root"
                , "Bdpp1234!"
        );
        conn.setAutoCommit(false);
        String sql = "insert into alert_realtime (id,hostname,value,timestamp,lastvalue,diffvalue,alertlevel,color,etl_time) values (?,?,?,?,?,?,?,?,?)";
        ps = conn.prepareStatement(sql);

    }

    @Override
    protected void invoke(Connection connection, AlertBean alertBean, Context context) throws Exception {
        System.err.println("start invoke.......");
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
//insert into alert_realtime (id,hostname,value,timestamp,lastvalue,diffvalue,alertlevel,color,etl_time) values (?,?,?,?,?,?,?,?,?)
        ps.setString(1,alertBean.getId());
        ps.setString(2,alertBean.getHostname());
        ps.setString(3,alertBean.getValue());
        ps.setString(4,alertBean.getTimestamp());
        ps.setString(5,alertBean.getLastvalue());
        ps.setString(6,alertBean.getDiffValue());
        ps.setString(7,alertBean.getColor());
        ps.setString(8,date);

        ps.addBatch();
        batch++;
        if(batch>=50){
            ps.execute();
            batch=0;
        }




    }

    @Override
    protected Connection beginTransaction() throws Exception {
        return null;
    }

    @Override
    protected void preCommit(Connection connection) throws Exception {
        System.err.println("start preCommit......."+connection);

    }

    @Override
    protected void commit(Connection connection) {
        try {
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void abort(Connection connection) {
        try {
            conn.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        if(conn!=null){
            conn.close();
        }
        if(ps!=null){
            ps.close();
        }
        super.close();
    }
}
