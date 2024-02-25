package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradePaymentBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName DwsTradePaymentSucWindowApp
 * @Package com.atguigu.gmall.realtime.dws.app
 * @Author CaiBW
 * @Create 24/02/22 下午 8:53
 * @Description
 */
@Slf4j
public class DwsTradePaymentSucWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTradePaymentSucWindowApp().start(
            10027,
            4,
            "dws_trade_payment_suc_window_app",
            Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //TODO 1. 清洗过滤，转换成JsonObject格式
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        //TODO 2. 按照user_id分组， 求独立用户
        SingleOutputStreamOperator<TradePaymentBean> cartAddUuStream = cartAddUu(etlStream);
        cartAddUuStream.print();
        //TODO 3. 分配水位线 , 开窗，聚合
        SingleOutputStreamOperator<TradePaymentBean> windowAggStream = windowAgg(cartAddUuStream);
//        //TODO 4. 写出到Doris
//        writeToDoris(windowAggStream);
    }
    
    private void writeToDoris(SingleOutputStreamOperator<TradePaymentBean> windowAggStream) {
        windowAggStream.map(
            new DorisMapFunction<>()
        ).sinkTo(
            FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_PAYMENT_SUC_WINDOW)
        );
    }
    
    private SingleOutputStreamOperator<TradePaymentBean> windowAgg(SingleOutputStreamOperator<TradePaymentBean> cartAddUuStream) {
        SingleOutputStreamOperator<TradePaymentBean> windowAggStream = cartAddUuStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<TradePaymentBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<TradePaymentBean>() {
                        @Override
                        public long extractTimestamp(TradePaymentBean element, long recordTimestamp) {
                            return element.getTs();
                        }
                    }
                )
                .withIdleness(Duration.ofSeconds(120L))
        ).windowAll(
            TumblingEventTimeWindows.of(Time.seconds(5L))
        ).reduce(
            new ReduceFunction<TradePaymentBean>() {
                @Override
                public TradePaymentBean reduce(TradePaymentBean value1, TradePaymentBean value2) throws Exception {
                    value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                    value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                    return value1;
                }
            },
            new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                @Override
                public void process(Context ctx, Iterable<TradePaymentBean> elements, Collector<TradePaymentBean> out) throws Exception {
                    TradePaymentBean bean = elements.iterator().next();
                    bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                    bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                    
                    bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));
                    
                    out.collect(bean);
                }
            }
        );
        return windowAggStream;
    }
    
    private SingleOutputStreamOperator<TradePaymentBean> cartAddUu(SingleOutputStreamOperator<JSONObject> etlStream) {
        SingleOutputStreamOperator<TradePaymentBean> cartAddUuStream = etlStream.keyBy(
            new KeySelector<JSONObject, String>() {
                @Override
                public String getKey(JSONObject value) throws Exception {
                    return value.getString("user_id");
                }
            }
        ).process(
            new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {
                
                private ValueState<String> lastPayDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<String> lastPayDesc = new ValueStateDescriptor<>("lastPayDesc", Types.STRING);
                    lastPayDesc.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                    lastPayDateState = getRuntimeContext().getState(lastPayDesc);
                }
                
                @Override
                public void processElement(JSONObject obj, Context ctx, Collector<TradePaymentBean> out) throws Exception {
                    String lastPayDate = lastPayDateState.value();
                    long ts = obj.getLong("ts");
                    String today = DateFormatUtil.tsToDate(ts);
                    
                    long payUuCount = 0L;
                    long payNewCount = 0L;
                    if (!today.equals(lastPayDate)) {
                        // 今天第一次支付成功
                        lastPayDateState.update(today);
                        payUuCount = 1L;
                        
                        if (lastPayDate == null) {
                            // 表示这个用户曾经没有支付过, 是一个新用户支付
                            payNewCount = 1L;
                        }
                    }
                    
                    if (payUuCount == 1) {
                        out.collect(new TradePaymentBean("", "", "", payUuCount, payNewCount, ts));
                    }
                    
                }
            });
        return cartAddUuStream;
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> etlStream = stream.flatMap(
            new FlatMapFunction<String, JSONObject>() {
                @Override
                public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                    try {
                        JSONObject jsonObj = JSONObject.parseObject(value);
                        String userId = jsonObj.getString("user_id");
                        Long ts = jsonObj.getLong("ts");
                        if (userId != null && ts != null) {
                            //将时间从秒 处理成 毫秒
                            jsonObj.put("ts", ts * 1000);
                            out.collect(jsonObj);
                        }
                    } catch (Exception e) {
                        log.warn("过滤掉脏数据: " + value);
                    }
                }
            }
        );
        return etlStream;
    }
}
