package com.atguigu.app.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        //避免namespace相关问题，添加上配置即可
        Properties properties = new Properties();
        properties.put("phoenix.schema.isNamespaceMappingEnabled","true");
        // com.atguigu.app.function.DimSinkFunction.open(DimSinkFunction.java:28)  ServerNotRunningYetException: Server is not running yet
        System.out.println("[DEBUG] start get phoenix conn...");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER, properties); // BUG01: 好像是这里一直获取不到conn, 经排查 Hbase Master因为Could not obtain block导致挂掉了, 重启HDFS解决.
        connection.setAutoCommit(true);
        System.out.println("[DEBUG] success get phoenix conn...");
    }

    // value:{"sinkTable": "dim_base_trademark", "database":"gmall-210325","before":{"tm_name":"atguigu","id":12},"after":{"tm_name":"Atguigu","id":12},"type":"update","tableName":"base_trademark"}
    //SQL：upsert into db.tn(id,tm_name) values('...','...')
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSQL = genUpsertSQL(sinkTable, after);
            System.out.println(upsertSQL);

            preparedStatement = connection.prepareStatement(upsertSQL);
            if ("update".equals(value.getString("type"))){
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
            }
            preparedStatement.execute();
        } catch (SQLException e){
            e.printStackTrace();
        } finally {
            if (preparedStatement != null ){
                preparedStatement.close();
            }
        }
    }

    //data:{"tm_name":"Atguigu","id":12}
    //SQL：upsert into db.tn(id,tm_name,aa,bb) values('...','...','...','...')
    private String genUpsertSQL(String sinkTable, JSONObject data) {
        Set<String> column = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(column, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
