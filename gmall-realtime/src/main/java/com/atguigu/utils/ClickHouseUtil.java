package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/*
create table visitor_stats_210325 (
    stt DateTime,
    edt DateTime,
    vc String,
    ch String,
    ar String,
    is_new String,
    uv_ct UInt64,
    pv_ct UInt64,
    sv_ct UInt64,
    uj_ct UInt64,
    dur_sum UInt64,
    ts UInt64
) engine = ReplacingMergeTree(ts)
partition by toYYYYMMDD(stt)
order by (stt,edt,is_new,vc,ch,ar);
*/

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getSink(String sql) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows // qst 不确定作用
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // 获取所有属性信息
                        Field[] fields = t.getClass().getDeclaredFields();
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            // 获取字段
                            Field field = fields[i];
                            field.setAccessible(true);

                            //获取字段上注解,解决表和JavaBean字段不一致情况
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null){
                                //存在该注解
                                offset++;
                                continue;
                            }

                            // 获取值; 通过反射获取 todo 需要案例去加深印象
                            Object o = field.get(t);

                            // 给预编译SQL对象赋值
                            // 第一次见，动态生成sql的value字段数
                            preparedStatement.setObject(i + 1 - offset, o);
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
