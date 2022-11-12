package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


// 测试例子
/*
{"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_16","os":"iOS 13.3.1","uid":"50","vc":"v2.1.134"},"page":{"during_time":12660,"page_id":"mine"},"ts":1608279370000}
增加5秒,不会捕获数据
{"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_16","os":"iOS 13.3.1","uid":"50","vc":"v2.1.134"},"page":{"during_time":12660,"page_id":"mine"},"ts":1608279375000}
增加9s，消费组会接收第一条数据
{"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_16","os":"iOS 13.3.1","uid":"50","vc":"v2.1.134"},"page":{"during_time":12660,"page_id":"mine"},"ts":1608279379000}
超时输出，要加12S，上面75000和79000数据都会接收到。
{"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_16","os":"iOS 13.3.1","uid":"50","vc":"v2.1.134"},"page":{"during_time":12660,"page_id":"mine"},"ts":1608279392000}


CEP要满足严格接收到两条，才会接收到第一条数据；例如这里
70000 -> 68000 水位线2秒延迟
75000 -> 73000，输入第二条时，不能说严格近邻，可能存在74000(76000)
79000 -> 77000, 接收到第一条数据，说明此时最小数据到了77000，那上面两条数据就严格近邻，就可以接收到第一条。

*/


//数据流：web/app -> nginx -> sprintBoot -> Kafka(ods) -> flink -> Kafka(dwd) -> flink -> kafka(dwm)
//程  序：mockLog -> nginx -> Logger.sh  -> Kafka(ZK) -> BaseLogApp -> kafka -> UserJumpDetailApp -> kafka
public class UserJumpDetailAppBak {
    public static void main(String[] args) throws Exception {
        //1.get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 依赖 Topic 分区

        //2. read data from kafka
        String sourceTopic = "dwd_page_log";
        String sourceGroupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kfDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, sourceGroupId));

        //3. 将每行数据转换成JSON对象并提取时间戳生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kfDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                        @Override
                        public long extractTimestamp(JSONObject element, long recordTimestamp) {
                            return element.getLong("ts");
                        }
                    })
                );

        //4. 定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        // 使用循环模式，定义模式序列
//        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                String lastPageId = value.getJSONObject("page").getString("last_page_id");
//                return lastPageId == null || lastPageId.length() <= 0;
//            }
//        }).times(2)
//                .consecutive() // 指定严格近邻
//            .within(Time.seconds(10));

        //5. 将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid")), pattern);

        //6. 提取匹配上的超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOut"){};
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                }
        );
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        //7. UNION 两种事件
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        //8. 将数据写入kafka
        unionDS.print("UserJumpDetailApp>>>>>>>>>>>");
        unionDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //9. 启动任务
        env.execute("UserJumpDetailApp");
    }
}
