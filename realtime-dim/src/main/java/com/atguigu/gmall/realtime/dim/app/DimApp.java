package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.*;

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
     * etl过滤
     */
    private static SingleOutputStreamOperator<JSONObject> getEtlStream(DataStreamSource<String> stream) {
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
        return etlStream;
    }

    /**
     * 读取配置表数据，动态处理Hbase维度表
     * 读取配置表数据，将CDC监控的配置表数据写入到TableProcessDim类中
     */
    private static SingleOutputStreamOperator<TableProcessDim> tableProcessDimStream(StreamExecutionEnvironment env) {
        DataStreamSource<String> configStream =
                env.fromSource(
                                FlinkSourceUtil.getMysqlSource(Constant.TABLE_PROCESS_DATABASE, Constant.TABLE_PROCESS_DIM),
                                WatermarkStrategy.noWatermarks(),
                                "mysqlsource")
                        .setParallelism(1);

        //configStream.print();

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
        ).setParallelism(1);
        return tableProcessDimStream;
    }

    /**
     * 在Hbase中建表or删表
     */
    private static SingleOutputStreamOperator<TableProcessDim> createOrDropTable(SingleOutputStreamOperator<TableProcessDim> tableProcessDimStream) {
        SingleOutputStreamOperator<TableProcessDim> createOrDropTableStream = tableProcessDimStream.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    // hbase连接对象
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
                            dropHbaseTable(tableProcessDim);
                        } else if ("u".equals(op)) {
                            //删除表
                            dropHbaseTable(tableProcessDim);
                            //建表
                            createHbaseTable(tableProcessDim);
                        } else {
                            // c r
                            //建表
                            createHbaseTable(tableProcessDim);
                        }
                        return tableProcessDim;
                    }

                    // 创建表
                    private void createHbaseTable(TableProcessDim tableProcessDim) throws IOException {
                        String[] cfs = tableProcessDim.getSinkFamily().split(",");
                        HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), cfs);
                    }

                    // 删除表
                    private void dropHbaseTable(TableProcessDim tableProcessDim) throws IOException {
                        HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                    }
                }
        ).setParallelism(1);
        return createOrDropTableStream;
    }

    /**
     * 处理topic_db中的维度数据
     */
    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterDimData(SingleOutputStreamOperator<JSONObject> etlStream, BroadcastStream<TableProcessDim> broadcastStream, MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = etlStream.connect(broadcastStream)
                .process(
                        new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

                            //预加载的配置表数据
                            private Map<String, TableProcessDim> preConfigMap;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                //基于生命周期open方法会早于processElement方法执行，
                                //可以在该方法中预加载配置表数据，存入到一个集合中 , 配合状态来使用
                                //Flink CDC 初始启动会比较慢， 主流数据早于配置流， 主流中本应该定性为维度表的数据 ， 因为状态中还没有存入维度表信息，
                                //而最终定性为非维度表数据

                                //通过jdbc编码完成预加载
                                java.sql.Connection connection = JdbcUtil.getConnection();
                                List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(
                                        connection,
                                        "select `source_table` , `sink_table` , `sink_family` , `sink_columns`, `sink_row_key` from gmall_config.table_process_dim ",
                                        TableProcessDim.class,
                                        true
                                );
                                //为了方便查询，转换成map结构
                                preConfigMap = new HashMap<>();

                                for (TableProcessDim tableProcessDim : tableProcessDimList) {
                                    preConfigMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                                }

                                JdbcUtil.closeConnection(connection);

                            }

                            @Override
                            public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                                //获取广播状态
                                ReadOnlyBroadcastState<String, TableProcessDim> readOnlyBroadcastState = ctx.getBroadcastState(mapStateDescriptor);
                                //从数据中获取表名
                                String tableName = jsonObj.getString("table");
                                //尝试从状态中获取
                                TableProcessDim tableProcessDim = readOnlyBroadcastState.get(tableName);

                                //根据tableProcessDim是否为空， 决定是否要从预加载的map中再次读取
                                if (tableProcessDim == null) {
                                    tableProcessDim = preConfigMap.get(tableName);


                                    log.info("从预加载的Map中读取维度信息");
                                }

                                if (tableProcessDim != null) {
                                    // 维度表的数据
                                    out.collect(Tuple2.of(jsonObj, tableProcessDim));
                                }
                            }

                            /**
                             * 处理广播流的数据
                             * <p>
                             * 如果 TableProcessDim的 op 是 d , 从广播状态中将当前维度表移除
                             * <p>
                             * 如果 TableProcessDim的 op 是 c , r , u , 将当前维度表添加到状态中
                             */
                            @Override
                            public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                                //System.out.println("DimApp.processBroadcastElement...........");
                                //获取广播状态
                                BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                                String op = tableProcessDim.getOp();
                                if ("d".equals(op)) {
                                    //从状态中移除维度表
                                    broadcastState.remove(tableProcessDim.getSourceTable());
                                    //同步删除预加载Map中的数据
                                    preConfigMap.remove(tableProcessDim.getSourceTable());
                                } else {
                                    // c r u
                                    //将维度表添加到状态中
                                    broadcastState.put(tableProcessDim.getSourceTable(), tableProcessDim);
                                }
                            }
                        }
                ).setParallelism(1);
        return dimStream;
    }

    /**
     * 过滤出需要写入到Hbase中的字段
     */
    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterSinkColumn(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterSinkColumnStream = dimStream.map(
                new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                        JSONObject jsonObj = value.f0;
                        TableProcessDim tableProcessDim = value.f1;

                        //从数据中提取到data
                        JSONObject dataObj = jsonObj.getJSONObject("data");
                        Set<String> dataAllKeys = dataObj.keySet();

                        //从tableProcessDim中获取sinkColumns
                        String sinkColumns = tableProcessDim.getSinkColumns();
                        List<String> sinkColumnList = Arrays.asList(sinkColumns.split(","));

                        //过滤出需要的字段
                        dataAllKeys.removeIf(key -> !sinkColumnList.contains(key));

                        return Tuple2.of(jsonObj, tableProcessDim);
                    }
                }
        );
        return filterSinkColumnStream;
    }

    /**
     * 将数据写出到Hbase的表中
     */
    private static void writeDimDataToHbase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterSinkColumnStream) {
        filterSinkColumnStream.addSink(
                new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
                    Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //获取Hbase的connection
                        connection = HBaseUtil.getConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        //关闭Hbase的connection
                        HBaseUtil.closeConnection(connection);
                    }

                    @Override
                    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
                        //将维度表的数据写入到Hbase中对应的维度表中
                        JSONObject jsonObj = value.f0;
                        TableProcessDim tableProcessDim = value.f1;
                        //写出的数据
                        JSONObject dataObj = jsonObj.getJSONObject("data");
                        //数据的操作类型
                        String type = jsonObj.getString("type");

                        //根据类型，决定是 put 还是 delete
                        if ("delete".equals(type)) {
                            //从hbase的维度表中删除维度数据
                            deleteDimData(dataObj, tableProcessDim);
                        } else {
                            //insert  update  bootstrap-insert
                            putDimData(dataObj, tableProcessDim);
                        }
                    }

                    private void putDimData(JSONObject dataObj, TableProcessDim tableProcessDim) throws IOException {
                        //rowkey
                        String sinkRowKeyName = tableProcessDim.getSinkRowKey();
                        String sinkRowKeyValue = dataObj.getString(sinkRowKeyName);

                        //cf
                        String cf = tableProcessDim.getSinkFamily();
                        HBaseUtil.putCells(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), sinkRowKeyValue, cf, dataObj);
                    }

                    private void deleteDimData(JSONObject dataObj, TableProcessDim tableProcessDim) throws IOException {
                        //rowkey
                        String sinkRowKeyName = tableProcessDim.getSinkRowKey();
                        String sinkRowKeyValue = dataObj.getString(sinkRowKeyName);
                        HBaseUtil.deleteCells(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), sinkRowKeyValue);
                    }
                }
        );
        System.out.println("=============================================================");
    }

    /**
     * 对于每个具体的App来讲， 只要关注核心逻辑的实现即可，  基本的环境准备、检查点设置等，在父类中已经全部实现.
     *
     * @param env       流环境
     * @param odsStream 数据流
     */
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> odsStream) {

        // TODO 1. 读取ODS层 topic_db的数据，
        //odsStream.print("ods");

        // TODO 2. 对读取的数据进行清洗过滤
        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(odsStream);
        //etlStream.print("etl");


        // TODO 3.读取配置表的数据， 动态处理 Hbase中的维度表
        SingleOutputStreamOperator<TableProcessDim> tableProcessDimStream = tableProcessDimStream(env);
        //tableProcessDimStream.print("table");


        // TODO 4. 在Hbase中建表or删表
        SingleOutputStreamOperator<TableProcessDim> createOrDropTableStream = createOrDropTable(tableProcessDimStream);
        //createOrDropTableStream.print("create");


        // TODO 5. 将配置流处理成广播流
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("mapStateDesc", Types.STRING, Types.POJO(TableProcessDim.class));
        BroadcastStream<TableProcessDim> broadcastStream = createOrDropTableStream.broadcast(mapStateDescriptor);

        // TODO 6. 处理topic_db中的维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = filterDimData(etlStream, broadcastStream, mapStateDescriptor);
        //dimStream.print("dim");

        // TODO 7. 过滤出需要写入到Hbase中的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterSinkColumnStream = filterSinkColumn(dimStream);
        //filterSinkColumnStream.print("filter");

        // TODO 8. 将数据写出到Hbase的表中
        writeDimDataToHbase(filterSinkColumnStream);
    }

}
