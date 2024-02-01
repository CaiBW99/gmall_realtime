package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @ClassName FlinkSinkUtil
 * @Package com.atguigu.gmall.realtime.common.util
 * @Author CaiBW
 * @Create 24/01/29 下午 4:29
 * @Description
 */
public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String topicName) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(topicName)
                                .setValueSerializationSchema(
                                        new SimpleStringSchema()
                                ).build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(topicName + "-" + System.currentTimeMillis())
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000")
                .build();
    }
    
    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
            .setBootstrapServers(Constant.KAFKA_BROKERS)
            .setRecordSerializer(
                new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> element, KafkaSinkContext context, Long timestamp) {
                        String topicName = element.f1.getSinkTable();
                        String data = element.f0.toJSONString();
                        return new ProducerRecord<>(topicName, data.getBytes());
                    }
                }
            )
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix("dwd_base_db-" + System.currentTimeMillis())
            .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000")
            .build();
    }
}
