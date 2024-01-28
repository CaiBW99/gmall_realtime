package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @ClassName BaseApp
 * @Package com.atguigu.gmall.realtime.common.base
 * @Author CaiBW
 * @Create 24/01/27 上午 11:47
 * @Description
 */
public abstract class BaseApp {
    /**
     * 子类实现，用于核心逻辑的处理
     */
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    /**
     * 控制整个代码的流程
     */
    public void start(int port, int parallelism, String ckAndGroupId, String topicName) {
        //操作HDFS用户
        System.setProperty("HADOOP_USER_NAME", Constant.HDFS_USER_NAME);

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度
        env.setParallelism(parallelism);

        // 状态后端的设置
        env.setStateBackend(new HashMapStateBackend());

        // 开启检查点以及检查点配置
        env.enableCheckpointing(2000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //  checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://linux1:8020/gmall/stream/" + ckAndGroupId);
        //  checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //  checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // checkpoint  的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        // 其他检查点配置

        // 准备KafkaSource
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topicName);

        // 读取数据
        DataStreamSource<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        //数据转换处理
        handle(env, stream);

        //启动执行
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
