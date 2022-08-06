package renaissance.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import renaissance.bean.CpuUseageHighAndLowBean;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;



public class CpuUsageHighAndLowSink extends TwoPhaseCommitSinkFunction<CpuUseageHighAndLowBean, Connection,Void> {

    public Connection conn = null;
    public PreparedStatement ps = null;
    int batch=0;
    public CpuUsageHighAndLowSink() {
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
        String sql = "insert into alert_realtime_batch_highandlow (id,hostname,highuse,lowuse,starttime,endtime,etltime) values (?,?,?,?,?,?,?)";
        ps = conn.prepareStatement(sql);
    }

    @Override
    protected void invoke(Connection transaction, CpuUseageHighAndLowBean bean, Context context) throws Exception {
        System.err.println("start invoke.......");
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        ps.setInt(1,bean.getId());
        ps.setString(2,bean.getHostname());
        ps.setInt(3,bean.getHighUse());
        ps.setInt(4,bean.getLowUse());
        ps.setString(5,bean.getStartTime());
        ps.setString(6,bean.getEndTime());
        ps.setString(7,date);


//        ps.addBatch();
//        batch++;
//        if(batch>=50){
        ps.execute();
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        System.out.println("cpuusageHighAndLow begintransaction");
        return null;
    }

    @Override
    protected void preCommit(Connection transaction) throws Exception {
        System.err.println("start preCommit......."+transaction);
    }

    @Override
    protected void commit(Connection transaction) {
        System.out.println("cpuuseageHighAndLow commit");
        try {
            if (conn!=null)
                conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void abort(Connection transaction) {
        System.out.println("cpuusageHighAndLow abort");
        try {
            if(conn!=null)
                conn.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        System.out.println("cpuusageHighAndLow close");



        if(conn!=null){
            conn.close();
        }
        if(ps!=null){
            ps.close();
        }
//        super.close();

    }
}
