package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;
import java.util.Date;


// 数据流：web/app -> nginx -> SpringBoot -> Kafka(ods)/logFile -> FlinApp -> Kafka(dwd) -> FlinkApp -> Kafka(DWM) -> FlinkApp -> CK(DWS)
// 程  序：mockLog -> Nginx -> Logger.sh  -> Kafka(ZK)         -> BaseLogApp -> kafka    -> uv/uj    -> kafka      -> VisitorStatsApp -> CK
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取kakfa数据创建流
        String groupId = "visitor_stats_app_210325";
        String uniqueVisitST = "dwm_unique_visit";
        String userJumpDetailST = "dwm_user_jump_detail";
        String pageViewST = "dwd_page_log";
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitST, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailST, groupId));
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewST, groupId));

        //3.将每个流处理成相同的数据类型
        //3.1处理UV数据
        SingleOutputStreamOperator<VisitorStats> vsWithUvDS = uvDS.map(line -> {
            JSONObject jo = JSON.parseObject(line);
            JSONObject common = jo.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jo.getLong("ts")
            );
        });

        //3.2处理UJ数据
        SingleOutputStreamOperator<VisitorStats> vsWithUjDS = ujDS.map(line -> {
            JSONObject jo = JSON.parseObject(line);
            JSONObject common = jo.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jo.getLong("ts")
            );
        });

        //3.3处理PV数据
        SingleOutputStreamOperator<VisitorStats> vsWithPvDS = pvDS.map(line -> {
            JSONObject jo = JSON.parseObject(line);
            JSONObject common = jo.getJSONObject("common");
            // 获取进入次数，持续访问时间
            JSONObject page = jo.getJSONObject("page");
            String last_page_id = page.getString("last_page_id");
            long sv = 0L;
            if (last_page_id == null || last_page_id.length() <= 0){
                sv = 1L;
            }
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, sv, 0L, page.getLong("during_time"),
                    jo.getLong("ts")
            );
        });

        //4.Union流
        DataStream<VisitorStats> unionDS = vsWithUvDS.union(vsWithUjDS, vsWithPvDS);

        //5.提取时间生成WaterMark
        // todo forBoundedOutOfOrderness 函数调用前面加类型的原因？
        // todo 为什么使用乱序WM
        SingleOutputStreamOperator<VisitorStats> vsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //6.按照维度信息分组
        // todo 聚合的原因是可能存在重复数据，KeySelector类不太熟悉；以及聚合流里面数据都有哪些方法？
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = vsWithWmDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<String, String, String, String>(
                        value.getVc(),
                        value.getCh(),
                        value.getAr(),
                        value.getIs_new()
                );
            }
        });

        //7.开窗聚合 10s的滚动窗口
        // todo 这里是盟的，https://www.bilibili.com/video/BV1Ju411o7f8?p=119
        // todo 开窗聚合，为什么要使用apply？使用process的区别？reduceFunction区别？
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> visitorStatsDS = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                // 获取窗口开始和结束时间，保存到宽表
                long start = window.getStart();
                long end = window.getEnd();

                VisitorStats next = input.iterator().next();

                next.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                next.setStt(DateTimeUtil.toYMDhms(new Date(end)));

                out.collect(next);
            }
        });

        //8.写入ck
        visitorStatsDS.print("visitorStatsDS>>>>>>>>>");
        // 基于JdbcSink.sink()实现工具类
        visitorStatsDS.addSink(ClickHouseUtil.getSink("insert into visitor_stats_210325 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //9.启动任务
        env.execute();
    }
}
