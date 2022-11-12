package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String queryString, Class<T> cls, boolean underScoreToCamel) throws Exception{
        //创建集合用于存放查询结果数据
        // rst sample: [{'c1':'v1', 'c2':'v2', ...}, {'c1':'v1', 'c2':'v2', ...}, ...]
        ArrayList<T> resultList = new ArrayList<>();
        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(queryString);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //解析resultSet
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()){ // 不断遍历行, 处理行中每列数据.
            //创建泛型对
            T t = cls.newInstance();
            //给泛型对象赋值
            for (int i = 1; i < columnCount + 1; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断是否需要转换为驼峰命名
                if (underScoreToCamel){
                    // 这里为啥不是UPPER_UNDERSCORE
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                //给获取列值
                Object value = resultSet.getObject(i);

                //给泛型对象赋值
                BeanUtils.setProperty(t, columnName, value);
            }
            //将对象加入集合
            resultList.add(t);
        }
        //返回结果集合
        return resultList;
    }

    public static void main(String[] args) throws Exception{
        // 测试
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Properties properties = new Properties();
        //避免namespace相关问题，添加上配置即可
        properties.put("phoenix.schema.isNamespaceMappingEnabled","true");
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER, properties);
        List<JSONObject> queryList = queryList(connection,
                "select * from GMALL210325_REALTIME.DIM_USER_INFO",
                JSONObject.class,
                true);
        for (JSONObject jsonObject :
                queryList) {
            System.out.println(jsonObject);
        }

        connection.close();
    }
}
