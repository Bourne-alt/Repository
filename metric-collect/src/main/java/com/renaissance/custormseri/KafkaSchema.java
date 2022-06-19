package com.renaissance.custormseri;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;



public class KafkaSchema implements DebeziumDeserializationSchema<String> {

    /**
     *
     * @param sourceRecord
     * @param collector
     * @throws Exception
     *  {"id":972795,"name":"mem.used","hostname":"svr1002","value":9686,"timestamp":1655012826}
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //创建json
        JSONObject result = new JSONObject();

        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");

        //
        Struct value = (Struct)sourceRecord.value();
        Struct after = value.getStruct("after");
//Struct{id=1122553,name=cpu.usage,hostname=svr1002,value=99999,timestamp=1655558465000}

        if(after !=null){
            //获取列信息
            result.put("id",after.get("id"));
            result.put("name",after.get("name"));
            result.put("hostname",after.get("hostname"));
            result.put("value",after.get("value"));
            result.put("timestamp",after.get("timestamp"));


        }


        collector.collect(result.toJSONString());




    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
