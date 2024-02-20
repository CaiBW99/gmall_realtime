package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

public abstract class MapDimFunction<T> extends RichMapFunction<T, T> implements DimFunction<T> {
    Connection connection;
    Jedis jedis;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取Hbase的连接
        connection = HBaseUtil.getConnection();
        jedis = RedisUtil.getJedis();
    }
    
    @Override
    public void close() throws Exception {
        //关闭连接
        HBaseUtil.closeConnection(connection);
        RedisUtil.closeJedis(jedis);
    }
    
    @Override
    public T map(T bean) throws Exception {
        
        JSONObject dimObj
            = RedisUtil.readDim(jedis, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(bean));
        if (dimObj == null) {
            System.out.println("走的Hbase: " + Constant.HBASE_NAMESPACE + ":" + getTableName() + ":" + getRowKey(bean));
            dimObj = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(bean));
            //存入到redis中
            RedisUtil.writeDim(jedis, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(bean), dimObj);
        } else {
            System.out.println("走的Redis: " + Constant.HBASE_NAMESPACE + ":" + getTableName() + ":" + getRowKey(bean));
        }
        
        addDim(bean, dimObj);
        
        return bean;
    }
    
}