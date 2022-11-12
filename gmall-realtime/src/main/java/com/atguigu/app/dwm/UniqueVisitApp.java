package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONAware;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;


// {"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_16","os":"iOS 13.3.1","uid":"50","vc":"v2.1.134"},"page":{"during_time":12660,"last_page_id":"home","page_id":"mine"},"ts":1608279381000}
// {"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_16","os":"iOS 13.3.1","uid":"50","vc":"v2.1.134"},"page":{"during_time":12660,"page_id":"mine"},"ts":1608279381000}
// {"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_16","os":"iOS 13.3.1","uid":"50","vc":"v2.1.134"},"page":{"during_time":12660,"page_id":"mine"},"ts":2308279381000}
// 数据流: web/app -> nginx -> SpringBoot -> Kafka(ods)-> FlinkApp -> Kafka(dwd) -> UniqueVisitApp(kafka) -> Kafka(dwm)
// 程序：mockLog -> Nginx -> Logger.sh -> Kafka(ZK) -> BaseLogApp -> kafka ->
// depend service: nginx, logger.sh(SpringBoot file2kafka), lg.sh(mock logData), kafka(zk), hdfs,
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.get kafka dwd_page_log
        String groupId = "unique_visit_app_210325";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //3.transfer json
        SingleOutputStreamOperator<JSONObject> JSONObjectDS = kafkaDS.map(JSON::parseObject);

        //4.filter stateProgram; 仅保留每个mid每天第一次登录数据
        KeyedStream<JSONObject, String> keyedStream = JSONObjectDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 设置状态的超时时间以及更新时间的方式
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // qst 未理解，解决9点清除时，8点和10点记录两次。
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dateState = getRuntimeContext().getState(valueStateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 取出上一条页面信息，判断是否为null
                // 为null时，取出状态数据，取出日期与当天对比，不相等时即为当天第一次登录
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
//                System.out.println(lastPageId);

                if (lastPageId == null || lastPageId.length() <= 0){
                    String lastDate = dateState.value();
                    String curDate = simpleDateFormat.format(value.getLong("ts"));
//                    System.out.println(lastDate);
//                    System.out.println(curDate);
                    if (!curDate.equals(lastDate)){  // 如果为空或者【不是今天】，说明今天还没访问过，则保留；
                        dateState.update(curDate);
                        return true;
                    }
                }
                return false;
            }
        });

        //5.sink
        uvDS.print("uvDS>>>>>");
        uvDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //6.execute
        env.execute("UniqueVisitApp");
    }
}
