package com.atgguigu.gmall.realtime.dwd.db.split.app.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName DwdBaseLogApp
 * @Package com.atguigu.gmall.realtime.dwd.db.split.app
 * @Author CaiBW
 * @Create 24/01/29 下午 4:31
 * @Description
 */
@Slf4j
public class DwdBaseLogApp extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLogApp().start(10011, 4, "dwd_base_log_app", Constant.TOPIC_LOG);
    }

    /**
     * 对数据进行清洗
     */
    private static SingleOutputStreamOperator<JSONObject> etlStream(DataStreamSource<String> odsStream) {
        SingleOutputStreamOperator<JSONObject> etlStream = odsStream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            //提取page
                            JSONObject pageObj = jsonObj.getJSONObject("page");
                            //提取start
                            JSONObject startObj = jsonObj.getJSONObject("start");
                            //mid
                            String mid = jsonObj.getJSONObject("common").getString("mid");
                            //ts
                            Long ts = jsonObj.getLong("ts");

                            if ((pageObj != null || startObj != null) && mid != null && ts != null) {
                                out.collect(jsonObj);
                            }

                        } catch (Exception e) {
                            log.warn("过滤脏数据：" + value);
                        }
                    }
                }
        );
        return etlStream;
    }

    /**
     * 新老用户状态修复
     */
    private static SingleOutputStreamOperator<JSONObject> fixIsNew(SingleOutputStreamOperator<JSONObject> etlStream) {
        SingleOutputStreamOperator<JSONObject> fixIsNewStream = etlStream.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {

                        return value.getJSONObject("common").getString("mid");
                    }
                }
        ).process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    // 定义状态，维护每个mid首次访问日期
                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 值状态描述
                        ValueStateDescriptor<String> firstVisitStateDesc = new ValueStateDescriptor<>("firstVisitStateDesc", Types.STRING);
                        // 获取状态
                        firstVisitDateState = getRuntimeContext().getState(firstVisitStateDesc);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 从状态中获取值
                        String firstVisitDate = firstVisitDateState.value();
                        // is_new
                        String isNew = value.getJSONObject("common").getString("is_new");
                        // 数据时间
                        Long ts = value.getLong("ts");
                        // 时间转换
                        String today = DateFormatUtil.tsToDate(ts);

                        // 新用户
                        if ("1".equals(isNew)) {
                            if (firstVisitDate == null) {
                                // 真实访客无需修改
                                firstVisitDateState.update(today);
                            }
                        } else if ("0".equals(isNew)) {
                            if (firstVisitDate == null) {
                                // 老访客，但数仓上线后第一次访问，将昨天日期状态存入状态
                                firstVisitDateState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                            } else {
                                // 老访客，状态有值，不做处理
                            }
                        } else {
                            // 非0非1不存在
                        }

                        out.collect(value);

                    }
                }
        );
        return fixIsNewStream;
    }

    /**
     * 日志数据分流
     */
    private static SingleOutputStreamOperator<String> splitStream(SingleOutputStreamOperator<JSONObject> fixIsNewStream, OutputTag<String> errTag, OutputTag<String> startTag, OutputTag<String> actionTag, OutputTag<String> displayTag) {
        SingleOutputStreamOperator<String> pageStream = fixIsNewStream.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        // 提取错误数据
                        JSONObject errObj = value.getJSONObject("err");
                        if (errObj != null) {
                            // 写到测流
                            ctx.output(errTag, errObj.toJSONString());
                            // 从数据中移除err
                            value.remove("err");
                        }

                        // 启动数据
                        JSONObject startObj = value.getJSONObject("start");
                        if (startObj != null) {
                            // 不拆分直接写出
                            ctx.output(startTag, value.toJSONString());
                        }

                        // 页面数据
                        JSONObject pageObj = value.getJSONObject("page");
                        JSONObject commonObj = value.getJSONObject("common");
                        Long ts = value.getLong("ts");
                        if (pageObj != null) {
                            // 分流actions数据
                            JSONArray actionArr = value.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionObj = actionArr.getJSONObject(i);
                                    // 补充common
                                    actionObj.put("common", commonObj);
                                    //补充page
                                    actionObj.put("page", pageObj);
                                    //补充ts
                                    actionObj.put("ts", ts);

                                    //写出
                                    ctx.output(actionTag, actionObj.toJSONString());
                                }
                            }
                            // 移除actions
                            value.remove("actions");

                            // 分流displays数据
                            JSONArray displaysArr = value.getJSONArray("displays");
                            if (displaysArr != null && displaysArr.size() > 0) {
                                for (int i = 0; i < displaysArr.size(); i++) {
                                    JSONObject displayObj = displaysArr.getJSONObject(i);
                                    //补充common
                                    displayObj.put("common", commonObj);
                                    //补充page
                                    displayObj.put("page", pageObj);
                                    //补充ts
                                    displayObj.put("ts", ts);

                                    //写出
                                    ctx.output(displayTag, displayObj.toJSONString());
                                }
                            }
                            // 移除displays
                            value.remove("displays");

                            // page数据
                            out.collect(value.toJSONString());
                        }
                    }
                }
        );
        return pageStream;
    }

    /**
     * 写出到kafka
     */
    private static void writeToKafka(SingleOutputStreamOperator<String> pageStream, SideOutputDataStream<String> errStream, SideOutputDataStream<String> startStream, SideOutputDataStream<String> actionStream, SideOutputDataStream<String> displayStream) {
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> odsStream) {
        // 核心业务处理
        // TODO 1.对数据进行清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etlStream(odsStream);
        //etlStream.print();

        // TODO 2.新老访客状态标记修复
        SingleOutputStreamOperator<JSONObject> fixIsNewStream = fixIsNew(etlStream);
        fixIsNewStream.print("fix");


        // TODO 3.日志数据分流
        //启动数据:    启动信息( common +  start )   错误信息
        //页面数据:    页面信息( common +  page )    曝光信息( common + page +  display )  动作信息(common + page + action )  错误信息

        OutputTag<String> errTag = new OutputTag<>("errTag", Types.STRING);
        OutputTag<String> startTag = new OutputTag<>("startTag", Types.STRING);
        OutputTag<String> displayTag = new OutputTag<>("displayTag", Types.STRING);
        OutputTag<String> actionTag = new OutputTag<>("actionTag", Types.STRING);

        // 页面信息流
        SingleOutputStreamOperator<String> pageStream = splitStream(fixIsNewStream, errTag, startTag, actionTag, displayTag);
        pageStream.print("page");
        // 错误信息流
        SideOutputDataStream<String> errStream = pageStream.getSideOutput(errTag);
        errStream.print("err");
        // 启动信息流
        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        startStream.print("start");
        // 曝光信息流
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        displayStream.print("dis");
        // 动作信息流
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);
        actionStream.print("act");

        // TODO 4.写出到kafka主题中
        writeToKafka(pageStream, errStream, startStream, actionStream, displayStream);
    }
}
