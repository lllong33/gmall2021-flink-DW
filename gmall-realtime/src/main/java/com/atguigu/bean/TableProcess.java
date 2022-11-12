package com.atguigu.bean;

import lombok.Data;

@Data
public class TableProcess {
    // 动态分流Sink常量
    public static final String SINK_TYPE_HBASE="hbase";
    public static final String SINK_TYPE_KAFKA="kafka";
    public static final String SINK_TYPE_CK="clickhouse";

    String sourceTable;        // 来源表
    String operateType;        // 操作类型insert,update,delete
    String sinkType;           // 输出类型hbase kafka
    String sinkTable;          // 输出表(主题)
    String sinkColumns;        // 输出字段
    String sinkPk;             // 主键字段
    String sinkExtend;         // 建表扩展

}
