package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/*
dwd_payment_info
{"callback_time":"2020-12-04 23:29:11","payment_type":"1103","out_trade_no":"329624256135819","create_time":"2020-12-04 23:28:51","user_id":3477,"total_amount":8463.00,"subject":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万超感知徕卡三摄 30倍数字变焦 6GB+128GB冰霜银全网通5G手机等4件商品","trade_no":"7341799493594792858916599593749928","id":17673,"order_id":26420}
{"callback_time":"2020-12-04 23:29:11","payment_type":"1102","out_trade_no":"225292976317253","create_time":"2020-12-04 23:28:51","user_id":3484,"total_amount":8334.00,"subject":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机等2件商品","trade_no":"9571768255823239958444336637626689","id":17674,"order_id":26421}

dwm_order_wide
{"activity_reduce_amount":0.00,"category3_id":477,"category3_name":"唇部","coupon_reduce_amount":0.00,"create_date":"2020-12-04","create_hour":"23","create_time":"2020-12-04 23:28:50","detail_id":79028,"feight_fee":10.00,"order_id":26420,"order_price":129.00,"order_status":"1001","original_total_amount":13696.00,"province_3166_2_code":"CN-ZJ","province_area_code":"330000","province_id":8,"province_iso_code":"CN-33","province_name":"浙江","sku_id":27,"sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Z02少女红 活力青春 璀璨金钻哑光唇膏 ","sku_num":2,"split_total_amount":258.00,"spu_id":9,"spu_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 ","tm_id":8,"tm_name":"索芙特","total_amount":13706.00,"user_age":482,"user_gender":"F","user_id":1543}
{"activity_reduce_amount":0.00,"category3_id":477,"category3_name":"唇部","coupon_reduce_amount":0.00,"create_date":"2020-12-04","create_hour":"23","create_time":"2020-12-04 23:28:50","detail_id":79028,"feight_fee":10.00,"order_id":26421,"order_price":129.00,"order_status":"1001","original_total_amount":13696.00,"province_3166_2_code":"CN-ZJ","province_area_code":"330000","province_id":8,"province_iso_code":"CN-33","province_name":"浙江","sku_id":27,"sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Z02少女红 活力青春 璀璨金钻哑光唇膏 ","sku_num":2,"split_total_amount":258.00,"spu_id":9,"spu_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 ","tm_id":8,"tm_name":"索芙特","total_amount":13706.00,"user_age":482,"user_gender":"F","user_id":1543}
* */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        //1.1设置CK&状态后端


        //2.读取Kfka主题数据，并将数据转为JavaBean对象，提取时间戳生成WaterMark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));

        //3.按照OrderID分组

        //4.双流Join
        SingleOutputStreamOperator<Object> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, Object>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<Object> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        //5.打印测试
//        paymentInfoDS.print("paymentInfoDS>>>");
//        orderWideDS.print("orderWideDS>>>");
        paymentWideDS.print("paymentWideDS>>>>>>");

        //6.sink Kafka
        paymentWideDS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        //7.execute
        env.execute("paymentWideDS");
    }
}
