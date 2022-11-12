package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.utils.CustomerDeserialization;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCWithCustomerSchema {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.source
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("node1")
                .port(3306)
                .username("root")
                .password("lf@123")
                .databaseList("gmall-210325-flink")
                .startupOptions(StartupOptions.initial()) // initial  # TODO latest()场景, 需要override deserialize
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        mysqlDS.print("mysqlDS>>>");

        // sink
        mysqlDS.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        // execute
        env.execute();

    }
}
