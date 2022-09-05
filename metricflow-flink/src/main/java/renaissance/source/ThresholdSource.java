package renaissance.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import renaissance.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ThresholdSource extends RichSourceFunction<String> {

    boolean isRunning =true;
    Connection conn=null;
    PreparedStatement preparedStatement=null;
    ResultSet resultSet=null;
    String sql="select * from metric_threshold where server_id = '2' and metric_name='mem.used'";
    @Override
    public void open(Configuration parameters) throws Exception {
       conn= JdbcUtils.getConn("bdp_master");
       preparedStatement=conn.prepareStatement(sql);


    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        while (isRunning){
            resultSet=preparedStatement.executeQuery();

            if(resultSet!=null && resultSet.next()){
                JSONObject results = new JSONObject();
                results.put("server_id",resultSet.getString("server_id"));
                results.put("amber_threshold",resultSet.getString("amber_threshold"));
                results.put("creation_time",resultSet.getString("creation_time"));
                results.put("red_threshold",resultSet.getString("red_threshold"));
                results.put("update_time",resultSet.getString("update_time"));
                results.put("metric_name",resultSet.getString("metric_name"));
                ctx.collect(results.toJSONString());
                System.out.println("threshold:"+results.toJSONString());
            }
            Thread.sleep(60*1000);
        }

    }

    @Override
    public void cancel() {
        isRunning=false;

    }

    @Override
    public void close() throws Exception {
        if(conn !=null) conn.close();
        if(preparedStatement !=null)  preparedStatement.close();
        if(resultSet!=null) resultSet.close();
    }
}
