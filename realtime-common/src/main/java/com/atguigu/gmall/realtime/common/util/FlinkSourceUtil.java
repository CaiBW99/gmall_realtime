package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * @ClassName FlinkSourceUtil
 * @Package com.atguigu.gmall.realtime.common.util
 * @Author CaiBW
 * @Create 24/01/27 上午 11:49
 * @Description flink的数据源获取工具类
 */
public class FlinkSourceUtil {

    /**
     * @param groupId   消费者组
     * @param topicName 主题
     * @return kafka source
     */
    public static KafkaSource<String> getKafkaSource(String groupId, String topicName) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(topicName)
                .setValueOnlyDeserializer(
                        new SimpleStringSchema()
                )
                // 方便测试， 不用每次都重新模拟生成数据
                .setStartingOffsets(OffsetsInitializer.earliest())
                //.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG , "read_committed")
                .build();
    }

    /**
     * @param databaseName 数据库名
     * @param tableName    表名
     * @return mysql source
     */
    public static MySqlSource<String> getMysqlSource(String databaseName, String tableName) {
        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList(databaseName)
                .tableList(databaseName + "." + tableName)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

    }
}
