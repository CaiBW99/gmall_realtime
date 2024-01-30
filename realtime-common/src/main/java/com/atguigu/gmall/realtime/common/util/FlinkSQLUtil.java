package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

/**
 * @ClassName FlinkSQLUtil
 * @Package com.atguigu.gmall.realtime.common.util
 * @Author CaiBW
 * @Create 24/01/30 上午 11:36
 * @Description
 */
public class FlinkSQLUtil {
    public static String getKafkaSourceDDL(String topicName, String groupId) {
        return "WITH(\n" +
                "     'connector'='kafka',\n" +
                "     'topic' = '" + topicName + "',\n" +
                "     'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "     'properties.group.id' = '" + groupId + "',\n" +
                "     'scan.startup.mode' = 'earliest-offset',\n" +
                "     'json.ignore-parse-errors' = 'true',\n" +
                "     'format' = 'json'\n" +
                ")";
    }

    public static String getKafkaSinkDDL(String topicName) {
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'sink.delivery-guarantee' = 'exactly-once',\n" +
                "  'sink.transactional-id-prefix' = '" + topicName + "-" + System.currentTimeMillis() + "'," +
                "  'properties.transaction.timeout.ms' = '600000', \n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static void main(String[] args) {
        String s = getKafkaSourceDDL("topic_db", "test");
        System.out.println(s);
    }
}
