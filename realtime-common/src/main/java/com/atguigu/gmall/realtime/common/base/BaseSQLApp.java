package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @ClassName BaseSQLApp
 * @Package com.atguigu.gmall.realtime.common.base
 * @Author CaiBW
 * @Create 24/01/30 上午 11:36
 * @Description
 */
public abstract class BaseSQLApp {
    //子类实现
    public abstract void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId);

    //代码实现流程
    public void start(int port, int parallelism, String ckAndGroupId) {
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

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 数据转换处理
        handle(tableEnv, env, ckAndGroupId);
    }

    public void readOdsTopicDb(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql(
                "CREATE TABLE topic_db(\n" +
                        "    `database` STRING,\n" +
                        "    `table`    STRING,\n" +
                        "    `type`     STRING,\n" +
                        "    `ts`       BIGINT,\n" +
                        "    `data`     MAP<STRING,STRING>,\n" +
                        "    `old`      MAP<STRING,STRING>,\n" +
                        "    `pt` AS PROCTIME(),\n" +
                        "    `et` AS TO_TIMESTAMP_LTZ(ts*1000,3),\n" +
                        "    watermark FOR et AS et - INTERVAL '5' SECOND\n" +
                        ") " + FlinkSQLUtil.getKafkaSourceDDL(Constant.TOPIC_DB, groupId)
        );
    }

    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(
                "CREATE TABLE base_dic (\n" +
                        " dic_code STRING,\n" +
                        " info ROW<dic_name STRING>,\n" +
                        " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'hbase-2.2',\n" +
                        " 'table-name' = 'gmall_realtime:dim_base_dic',\n" +
                        " 'zookeeper.quorum' = '" + Constant.HBASE_ZOOKEEPER_QUORUM + "'\n" +
                        ");"
        );
    }
}
