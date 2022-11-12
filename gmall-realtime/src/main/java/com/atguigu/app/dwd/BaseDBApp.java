package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.function.DimSinkFunction;
import com.atguigu.app.function.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.CustomerDeserialization;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

// 数据流：web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim)
// 程序：mockDb -> mysql -> FlinkCDC -> Kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(hbase, zk, hdfs)
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // 1. get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.consume kafka ods_base_db topic dataStream
        String sourceTopic = "ods_base_db";
        String groupID = "ods_dwd_base_db_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupID));

        // 3. per line transform JSONObject and filter dirty data
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject).filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value){
                String type = value.getString("type");
                return ! type.equals("delete");
            }
        });
//        jsonObjDS.print("[DEBUG] input stream:");

        // 4. use flinkcdc consume config table and process to broadcast stream.
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("node1")
                .port(3306)
                .username("root")
                .password("lf@123")
                .databaseList("gmall-210325-realtime")
                .tableList("gmall-210325-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
//        tableProcessStrDS.print("[DEBUG] dimConfigStream");

        // transform broadcast stream
        // 如何构建 MapStateDescriptor?
        /* table_process 满足分流的字段：
            sourceTable 表名，type 新增或变化，sinkType，sinkTable,      sinkColumns 建表需要的字段,     pk 建表主键,     extend 建表其他信息
            eg:
            base_trademark    insert        hbase   dim_xxxx(Phoenix表名)
            order_info        insert        kafka   dwd_xxxa(kafka主题名)
            order_info        update        kafka   dwd_xxxb
            主键：用于 key 区分每条数据，sourceTable+type
            phoenix 表需要提前创建
            JavaBean 映射便于处理
        * */
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);
//        System.out.println(broadcastStream.toString());


        // 5. join MainFlow and broadcastFlow
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);
//        System.out.println(connectedStream.toString());

        // 6. 分流 处理数据 广播流数据 主流数据
        /*
        * 广播流：processBroadcastElement
        * 1.解析数据 Strin => TableProcess
        * 2.检查HBASE表是否存在并建表
        * 3.写入状态
        *
        * 主流：processElement
        * 1.获取广播的配置数据
        * 2.过滤数据
        * 3.分流
        *
        * 参数：状态，描述器
        * */
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {};
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        // 7. 提取 kafka 流数据和 HBase 数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);
        kafka.print("Kafka>>>>>>>>>"); // TODO 比较有意思点, 重启该task, 如何确定kafka消费的位置?
        hbase.print("Hbase>>>>>>>>>");

        // 8. 将 Kafka 数据写入 kafka 主题，将 Hbase 数据写入 Phoenix 表
        hbase.addSink(new DimSinkFunction());
        kafka.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sinkTable"),
                        element.getString("after").getBytes());
            }
        }));

        // 9. start task
        env.execute("BaseDBApp");


    }
}
