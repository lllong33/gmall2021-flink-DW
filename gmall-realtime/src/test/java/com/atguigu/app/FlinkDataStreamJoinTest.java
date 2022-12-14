package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/*
测试步骤：
node1 输入 1001,zhangsan,1
node2 输入 1001,male,1
    返回 (Bean1(id=1001, name=zhangsan, ts=1),Bean2(id=1001, name=male, ts=1))
node2 输入 1001,female,2
    (Bean1(id=1001, name=zhangsan, ts=1),Bean2(id=1001, name=female, ts=2))
node1: 1002,lisi,10
node2: 1001,male,3
    (Bean1(id=1001, name=zhangsan, ts=1),Bean2(id=1001, name=male, ts=3))
node2: 1002,female,10
    (Bean1(id=1002, name=lisi, ts=10),Bean2(id=1002, name=female, ts=10))
node2: 1001,female,3
    无返回，原因是已经过了水位线

问题：还是无法理解这个场景和原理，仅仅只能复现

* */

public class FlinkDataStreamJoinTest {
    public static void main(String[] args) throws Exception {
        //1.get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.get source and amke watermark
        SingleOutputStreamOperator<Bean1> stream1 = env.socketTextStream("node1", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                            @Override
                            public long extractTimestamp(Bean1 element, long recordTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }));

        SingleOutputStreamOperator<Bean2> stream2 = env.socketTextStream("node2", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                    @Override
                    public long extractTimestamp(Bean2 element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        //3.双流join
        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> joinDS = stream1.keyBy(Bean1::getId)
                .intervalJoin(stream2.keyBy(Bean2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
                    @Override
                    public void processElement(Bean1 left, Bean2 right, Context ctx, Collector<Tuple2<Bean1, Bean2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        //4.打印
        joinDS.print();

        //5.启动任务
        env.execute("FlinkDataStreamJoinTest");

    }
}
