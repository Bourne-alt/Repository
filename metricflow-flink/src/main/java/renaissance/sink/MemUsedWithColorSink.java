package renaissance.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import renaissance.bean.CpuUseageHighAndLowBean;
import renaissance.bean.MemUsedWithColorBean;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class MemUsedWithColorSink extends TwoPhaseCommitSinkFunction<MemUsedWithColorBean, Connection,Void> {

    public Connection conn = null;
    public PreparedStatement ps = null;
    int batch=0;
    public MemUsedWithColorSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = DriverManager.getConnection(
                "jdbc:mysql://loadbalancer1:3306/bdp_metric?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true"
                , "root"
                , "Bdpp1234!"
        );

      //  beanMemUsedWithColorBean{id=9806588, hostname='svr1001', alertLev='red', threhUpdateTime='2018-09-01 00:00:00', threhCreateTime='2018-09-01 00:00:00', amberThreshold=5120, value='26647', redThreshold=5760, metricName='mem.used'}
        conn.setAutoCommit(false);
        String sql = "insert into alert_level_real_time (id,hostname,metricname,alertlev,timehrehupdatetime,threhcreatetime,amberthreshold," +
                "redthreshold,etltime,value) values (?,?,?,?,?,?,?,?,?,?)";
        ps = conn.prepareStatement(sql);
    }

    @Override
    protected void invoke(Connection transaction, MemUsedWithColorBean bean, Context context) throws Exception {
        System.err.println("start invoke.......");
        System.out.println("bean"+bean.toString());
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        ps.setInt(1,bean.getId());
        ps.setString(2,bean.getHostname());
        ps.setString(3,bean.getMetricName());
        ps.setString(4,bean.getAlertLev());
        ps.setString(5,bean.getThrehUpdateTime()!=null?bean.getThrehUpdateTime():"-");
        ps.setString(6,bean.getThrehCreateTime()!=null?bean.getThrehCreateTime():"-");
        ps.setInt(7,bean.getAmberThreshold());
        ps.setInt(8,bean.getRedThreshold());
        ps.setString(9,date);
        ps.setString(10,bean.getValue());
        ps.addBatch();

        batch++;
        if(batch>=50) {
            ps.execute();
            batch=0;
        }
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        System.out.println("MemUsedWithColorSink begintransaction");
        return null;
    }

    @Override
    protected void preCommit(Connection transaction) throws Exception {
        System.err.println("start preCommit......."+transaction);
    }

    @Override
    protected void commit(Connection transaction) {
        System.out.println("MemUsedWithColorSink commit");
        try {
            if (conn!=null)
                conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void abort(Connection transaction) {
        System.out.println("MemUsedWithColorSink abort");
        try {
            if(conn!=null)
                conn.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        System.out.println("MemUsedWithColorSink close");



        if(conn!=null){
            conn.close();
        }
        if(ps!=null){
            ps.close();
        }
//        super.close();

    }
}
