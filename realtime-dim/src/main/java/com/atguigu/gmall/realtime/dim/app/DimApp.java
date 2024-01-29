package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @ClassName DimApp
 * @Package com.atguigu.gmall.realtime.dim.app
 * @Author CaiBW
 * @Create 24/01/28 下午 7:22
 * @Description 核心逻辑处理
 * 1. 读取ODS层 topic_db的数据，
 * 2. 对读取的数据进行清洗过滤
 * 3.  读取配置表的数据， 动态处理 Hbase中的维度表
 * 4. 在Hbase中建表or删表
 * 5. 建配置流处理成广播流
 * 6. 处理topic_db中的维度数据
 * 7. 过滤出需要写入到Hbase中的字段
 * 8. 将数据写出到Hbase的表中
 */
@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    /**
     * 对于每个具体的App来讲， 只要关注核心逻辑的实现即可，  基本的环境准备、检查点设置等，在父类中已经全部实现.
     *
     * @param env
     * @param stream
     */
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1. 读取ODS层 topic_db的数据，

        //2. 对读取的数据进行清洗过滤
        SingleOutputStreamOperator<JSONObject> etlStream = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        //将json字符串数据解析成Json对象
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            //提取库名
                            String database = jsonObject.getString("database");
                            String type = jsonObject.getString("type");
                            JSONObject dataObj = jsonObject.getJSONObject("data");

                            if ("gmall".equals(database) &&
                                    !"bootstrap-start".equals(type)
                                    && !"bootstrap-complete".equals(type)
                                    && dataObj != null
                                    && dataObj.size() > 0
                            ) {
                                //将数据写出
                                out.collect(jsonObject);
                            }

                        } catch (Exception e) {
                            log.warn("过滤掉脏数据: " + value);
                        }
                    }
                }
        );

        etlStream.print("etl");


        //3.  读取配置表的数据， 动态处理 Hbase中的维度表
        DataStreamSource<String> configStream =
                env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.TABLE_PROCESS_DATABASE, Constant.TABLE_PROCESS_DIM), WatermarkStrategy.noWatermarks(), "mysqlsource");
        SingleOutputStreamOperator<TableProcessDim> tableProcessDimStream = configStream.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String value) throws Exception {
                        JSONObject jsonObj = JSONObject.parseObject(value);
                        //提取操作类型：  c  r   u  d
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim;

                        if ("d".equals(op)) {
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            // c r u
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }

                        //补充操作类型
                        tableProcessDim.setOp(op);

                        return tableProcessDim;
                    }
                }
        );


        //4. 在Hbase中建表or删表
        SingleOutputStreamOperator<TableProcessDim> createOrDropTableStream = tableProcessDimStream.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        connection = HBaseUtil.getConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeConnection(connection);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        String op = tableProcessDim.getOp();
                        //根据op的值，决定建表还是删表
                        if ("d".equals(op)) {
                            //删除表
                            dropHBaseTable(tableProcessDim);
                        } else if ("u".equals(op)) {
                            //删除表
                            dropHBaseTable(tableProcessDim);
                            //建表
                            createHBaseTable(tableProcessDim);
                        } else {
                            // c r
                            //建表
                            createHBaseTable(tableProcessDim);
                        }
                        return tableProcessDim;
                    }

                    private void createHBaseTable(TableProcessDim tableProcessDim) throws IOException {
                        String[] cfs = tableProcessDim.getSinkFamily().split(",");
                        HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), cfs);
                    }

                    private void dropHBaseTable(TableProcessDim tableProcessDim) throws IOException {
                        HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                    }
                }
        );

        //5. 建配置流处理成广播流
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("mapStateDesc", Types.STRING, Types.POJO(TableProcessDim.class));
        BroadcastStream<TableProcessDim> broadcastStream = createOrDropTableStream.broadcast(mapStateDescriptor);

        //6. 处理topic_db中的维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = etlStream.connect(broadcastStream)
                .process(
                        new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
                            @Override
                            public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                                //获取广播状态
                                ReadOnlyBroadcastState<String, TableProcessDim> readOnlyBroadcastState = ctx.getBroadcastState(mapStateDescriptor);
                                //从数据中获取表名
                                String tableName = jsonObj.getString("table");
                                //尝试从状态中获取
                                TableProcessDim tableProcessDim = readOnlyBroadcastState.get(tableName);

                                if (tableProcessDim != null) {
                                    // 维度表的数据
                                    out.collect(Tuple2.of(jsonObj, tableProcessDim));
                                }
                            }

                            // 处理广播流的数据, 如果 TableProcessDim的 op 是 d , 从广播状态中将当前维度表移除, 如果 TableProcessDim的 op 是 c , r , u , 将当前维度表添加到状态中
                            @Override
                            public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {

                                //获取广播状态
                                BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                                String op = tableProcessDim.getOp();
                                if ("d".equals(op)) {
                                    //从状态中移除维度表
                                    broadcastState.remove(tableProcessDim.getSourceTable());
                                } else {
                                    // c r u
                                    //将维度表添加到状态中
                                    broadcastState.put(tableProcessDim.getSourceTable(), tableProcessDim);
                                }

                            }
                        }
                );

        dimStream.print("dim");


        //7. 过滤出需要写入到Hbase中的字段

        //8. 将数据写出到Hbase的表中
    }

}
