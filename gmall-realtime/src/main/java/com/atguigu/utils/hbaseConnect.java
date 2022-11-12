package com.atguigu.utils;

import com.atguigu.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class hbaseConnect {
    public static Connection getConnection() throws Exception {
        // 测试
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Properties properties = new Properties();
        //避免namespace相关问题，添加上配置即可
        properties.put("phoenix.schema.isNamespaceMappingEnabled","true");
        System.out.println("[DEBUG] start get phoenix conn");
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER, properties);
        System.out.println("[DEBUG] success get phoenix conn");
        return connection;
    }
}
