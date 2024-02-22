package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.CartAddUuBean;
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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName DwsTradeCartAddUuWindowApp
 * @Package com.atguigu.gmall.realtime.dws.app
 * @Author CaiBW
 * @Create 24/02/19 上午 9:37
 * @Description
 */
@Slf4j
public class DwsTradeCartAddUuWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindowApp().start(
            10026,
            4,
            "dws_trade_cart_add_uu_window_app",
            Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //stream.print() ;
        //1. 清洗过滤，转换成JsonObject格式
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        
        //etlStream.print() ;
        
        //2. 按照user_id分组， 求独立用户
        SingleOutputStreamOperator<CartAddUuBean> cartAddUuStream = cartAddUu(etlStream);
        
        //cartAddUuStream.print();
        
        //3. 分配水位线 , 开窗，聚合
        SingleOutputStreamOperator<CartAddUuBean> windowAggStream = widowAgg(cartAddUuStream);
        
        //windowAggStream.print();
        
        //4. 写出到Doris
        writeToDoris(windowAggStream);
    }
    
    private static void writeToDoris(SingleOutputStreamOperator<CartAddUuBean> windowAggStream) {
        windowAggStream.map(
            new DorisMapFunction<>()
        ).sinkTo(
            FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_CART_ADD_UU_WINDOW)
        );
    }
    
    private static SingleOutputStreamOperator<CartAddUuBean> widowAgg(SingleOutputStreamOperator<CartAddUuBean> cartAddUuStream) {
        SingleOutputStreamOperator<CartAddUuBean> windowAggStream = cartAddUuStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<CartAddUuBean>() {
                        @Override
                        public long extractTimestamp(CartAddUuBean element, long recordTimestamp) {
                            return element.getTs();
                        }
                    }
                )
        ).windowAll(
            TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        ).reduce(
            new ReduceFunction<CartAddUuBean>() {
                @Override
                public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                    value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                    return value1;
                }
            }
            ,
            new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                @Override
                public void process(ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>.Context context, Iterable<CartAddUuBean> elements, Collector<CartAddUuBean> out) throws Exception {
                    CartAddUuBean cartAddUuBean = elements.iterator().next();
                    
                    //补充窗口信息
                    cartAddUuBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                    cartAddUuBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                    //补充分区信息
                    cartAddUuBean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));
                    
                    //写出数据
                    out.collect(cartAddUuBean);
                }
            }
        );
        return windowAggStream;
    }
    
    private static SingleOutputStreamOperator<CartAddUuBean> cartAddUu(SingleOutputStreamOperator<JSONObject> etlStream) {
        SingleOutputStreamOperator<CartAddUuBean> cartAddUuStream = etlStream.keyBy(
            new KeySelector<JSONObject, String>() {
                @Override
                public String getKey(JSONObject value) throws Exception {
                    return value.getString("user_id");
                }
            }
        ).process(
            new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
                
                private ValueState<String> lastAddDtState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<String> lastAddDesc = new ValueStateDescriptor<>("lastAddDesc", Types.STRING);
                    lastAddDesc.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                    lastAddDtState = getRuntimeContext().getState(lastAddDesc);
                }
                
                @Override
                public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, CartAddUuBean>.Context ctx, Collector<CartAddUuBean> out) throws Exception {
                    //从状态中取值
                    String lastAddDt = lastAddDtState.value();
                    //today
                    Long ts = value.getLong("ts");
                    String todayDt = DateFormatUtil.tsToDate(ts);
                    
                    //判断是否为独立用户
                    if (!todayDt.equals(lastAddDt)) {
                        
                        CartAddUuBean cartAddUuBean = CartAddUuBean.builder()
                            .cartAddUuCt(1L)
                            .ts(ts)
                            .build();
                        
                        //更新状态
                        lastAddDtState.update(todayDt);
                        
                        //写出数据
                        out.collect(cartAddUuBean);
                    }
                }
            }
        );
        return cartAddUuStream;
    }
    
    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
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
