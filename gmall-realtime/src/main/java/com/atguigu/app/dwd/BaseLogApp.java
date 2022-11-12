package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// 数据流: web/app -> nginx -> SpringBoot -> Kafka(ods)-> FlinkApp -> Kafka(dwd)
// 程序：mockLog -> Nginx -> Logger.sh -> Kafka(ZK) -> BaseLogApp -> kafka
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 消费ods_base_log, source
        String sourceTopic = "ods_base_log";
        String groupId = "ods_dwd_base_log_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // process
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject);
        // 3.每行数据转JSON，并处理脏数据
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    // 将数据写入测输出流
                    ctx.output(outputTag, value);
                }
            }
        });
        System.out.println("ods data transform jsonObjDS");
//        jsonObjDS.print("jsonObjDS>>>>");

        // 4.新老用户校验 状态编程
        // 可能存在卸载重装用户, 这里通过mid历史状态判断(flink状态会持久化，记录用户mid状态, 这里valueState是针对每个mid创建的？). 重置is_new为0
        // todo: RichMapFunction，ValueState
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid")) // 按照mid分组
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String isNew = value.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {
                            String state = valueState.value();
                            if (state != null) {
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("1");
                            }
                        }
                        return value;
                    }
                });
        System.out.println("jsonObjDS to jsonObjWithNewFlagDS");
//        jsonObjWithNewFlagDS.print("jsonObjWithNewFlagDS>>>>>>>>>>>>>");

        // 5. 分流；
        // 主流：页面；
        // 测输出流：启动、曝光
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                // 启动
                System.out.println(value.toString());
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    ctx.output(startTag, value.toJSONString());
                } else {
                    // 将数据写入页面日志主流
                    out.collect(value.toJSONString());

                    // 曝光
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        // get page_id
                        String pageId = value.getJSONObject("page").getString("page_id");
                        // displays 只有具体信息, 这里加上 page_id
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            // add page_id
                            display.put("page_id", pageId);
                            // sink ctx
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });
        System.out.println("jsonObjWithNewFlagDS to pageDS");

        // 6. 提取测输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        // 7. sink kafka
        startDS.print("Start>>>>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>>>>>");
        pageDS.print("Page>>>>>>>>>>>>>>");

        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));

        // 8.startup
        env.execute("BaseLogApp");

    }
}
