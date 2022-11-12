package com.atguigu.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.ObjectOutput;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        System.out.println("[DEBUG] init instance");
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // TODO 为啥这里进不来? 重启下服务?
        System.out.println("[DEBUG] start init hbase conn");
        Class.forName(GmallConfig.PHOENIX_DRIVER); // 将驱动类装载、连接、初始化
        Properties properties = new Properties();
        //避免namespace相关问题，添加上配置即可
        properties.put("phoenix.schema.isNamespaceMappingEnabled","true");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER, properties);

        // todo 避免先启动flinkcdc时，配置表未创建，在open中初始化一次建表
            // 如何获取到侧输出流数据 value，解析成JSONObject拿到创建需要的参数? 直接读mysql表即可
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1. 获取状态数据；2.过滤字段；3.分流
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null){
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            // 将输出表/主题信息写入value
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                ctx.output(objectOutputTag, value);
            }
        } else {
            System.out.println("present Key: " + key + " not exists! ");
        }
    }

    /**
     *
     * @param data      {"id": "11", "tm_name": "atguigu", "logo_url": "aaa"}
     * @param sinkColumns id,tm_name
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        List<String> columns = Arrays.asList(sinkColumns.split(","));
        //        while(iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columns.contains(next.getKey())){
//                iterator.remove();
//            }
//        }
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }

    // value: {"db": "", "tn": "", "before": {}, "after": {}, "type": ""}
    // 上述格式是 FlinkCDC 数据传输结构
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        System.out.println("[DEBUG] start processBroadcastElement");
        // 1. 获取并解析数据，2.建表  3.写入状态，广播出去
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        // qst 这里写入状态是放入到 BroadcastState
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
        System.out.println("[DEBUG] tableProcess write to BroadCastState: "+tableProcess.toString());
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        System.out.println("[DEBUG] create table if not exists: "+ sinkTable);
        PreparedStatement preparedStatement = null;
        try{
            // 建表语句：create table if not exists db.tn(id varchar primary key, tm_name varchar) xxx;
            // 处理 pk, extend 为null情况
            if (sinkPk == null) sinkPk="id";
            if (sinkExtend == null) sinkExtend="";
            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");
            List<String> pkLst = Arrays.asList(sinkPk.split(","));
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];

                // 主键/联合主键，逗号

                if (pkLst.contains(field)) {
                    createTableSQL.append(field).append(" varchar primary key");
                } else{
                    createTableSQL.append(field).append(" varchar");
                }
                if (i < fields.length - 1) {
                    createTableSQL.append(", ");
                }
            }
            createTableSQL.append(")").append(sinkExtend);
            System.out.println(createTableSQL);
            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());

            preparedStatement.execute();
        } catch (SQLException e){
            throw new RuntimeException("Phoenix table" + sinkTable + "create failed!");
        } finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
