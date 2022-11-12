package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception{
        // 查询Phoenix之前先查Redis
        Jedis jedis = RedisUtil.getJedis();
        // DIM:DIM_USER_INFO:143
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null){
            jedis.expire(redisKey, 24*60*60);
            jedis.close();
            return JSONObject.parseObject(dimInfoJsonStr);
        }
        // join query statement
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";

        // query
        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJson = jsonObjects.get(0);

        // 返回前写入到Redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24*60*60);
        jedis.close();

        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Connection connection = hbaseConnect.getConnection();
        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "1007"));
        long end = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "1007"));
        long end2 = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "1007"));
        long end3 = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println(end2 - end);
        System.out.println(end3 - end2);

        // 测试update是否会删除redis数据
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "15"));

        connection.close();
    }
}
