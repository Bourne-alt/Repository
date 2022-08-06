package renaissance.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CpuUsageHighAndLowSink extends RichSinkFunction<String> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
}
