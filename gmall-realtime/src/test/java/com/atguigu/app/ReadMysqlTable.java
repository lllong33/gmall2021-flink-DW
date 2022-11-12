package com.atguigu.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.JdbcUtil;
import com.atguigu.utils.hbaseConnect;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class ReadMysqlTable {
    // 读mysql表中配置信息，创建 phoenix
    public static void main(String[] args) throws Exception {
        readMysqlSinkHbase();

    }

    public static void readMysqlSinkHbase() throws Exception{
        Connection connect = null;
        Statement statement = null;
        ResultSet resultSet = null;
        Class.forName("com.mysql.jdbc.Driver");
        connect = DriverManager.getConnection("jdbc:mysql://node1:3306/gmall-210325-realtime?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root",
                "lf@123");
        String query="SELECT * FROM `table_process`";
        List<JSONObject> jsonObjects = JdbcUtil.queryList(connect, query, JSONObject.class, false);
        System.out.println(jsonObjects.size());

        for (JSONObject json :
                jsonObjects) {
            System.out.println(json.toString());
            TableProcess tableProcess = JSON.parseObject(json.toString(), TableProcess.class);
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }
    }

    private static void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws Exception {
        // todo 这里可以优化，功能拆分，代码复用
        PreparedStatement preparedStatement = null;
        Connection connection = hbaseConnect.getConnection();
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

    public static void readMysql() throws Exception{
        Connection connect = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connect = DriverManager.getConnection("jdbc:mysql://node1:3306/gmall-210325-realtime?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                    "root",
                    "lf@123");
            statement = connect.createStatement();
            resultSet = statement.executeQuery("SELECT * FROM `table_process`");
            while (resultSet.next()) {
                System.out.println(resultSet.toString());
            }
            resultSet.close();
            statement.close();
            connect.close();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (resultSet != null){
                resultSet.close();
            }
            if (statement != null){
                statement.close();
            }
            if (connect != null){
                connect.close();
            }
        }
    }
}
