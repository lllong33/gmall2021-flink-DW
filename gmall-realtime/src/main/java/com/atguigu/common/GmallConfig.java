package com.atguigu.common;

public class GmallConfig {
    //Phoenix 库名
    public static String HBASE_SCHEMA="GMALL210325_REALTIME"; // 注意需要提前创建

    public static final String PHOENIX_DRIVER="org.apache.phoenix.jdbc.PhoenixDriver";

    public static final String PHOENIX_SERVER="jdbc:phoenix:node1,node2,node3:2181";

    public static final String CLICKHOUSE_DRIVER="ru.yandex.clickhouse.ClickHouseDriver";

    public static final String CLICKHOUSE_URL="jdbc:clickhouse://node1:8123/default";


}
