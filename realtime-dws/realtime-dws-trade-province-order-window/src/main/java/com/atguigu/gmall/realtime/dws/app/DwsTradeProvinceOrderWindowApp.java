package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.AsyncMapDimFunction;
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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName DwsTradeProvinceOrderWindowApp
 * @Package com.atguigu.gmall.realtime.dws.app
 * @Author CaiBW
 * @Create 24/02/22 下午 1:53
 * @Description
 */
@Slf4j
public class DwsTradeProvinceOrderWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindowApp().start(10030, 4, "dws_trade_province_order_window_app", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //TODO 1.清洗过滤
        //stream.print();
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        //etl.print();
        
        //TODO 2.去重
        SingleOutputStreamOperator<TradeProvinceOrderBean> duplicationStream = duplication(etlStream);
        
        //TODO 3.分配水位线
        SingleOutputStreamOperator<TradeProvinceOrderBean> windowAggStream = windowAgg(duplicationStream);
        
        //TODO 4.维度关联
        SingleOutputStreamOperator<TradeProvinceOrderBean> joinStream = join(windowAggStream);
        
        //TODO 5.写出到doris
        writeToDoris(joinStream);
    }
    
    private SingleOutputStreamOperator<TradeProvinceOrderBean> join(SingleOutputStreamOperator<TradeProvinceOrderBean> windowAggStream) {
        SingleOutputStreamOperator<TradeProvinceOrderBean> joinStream = AsyncDataStream.unorderedWait(
            windowAggStream,
            new AsyncMapDimFunction<TradeProvinceOrderBean>() {
                @Override
                public String getTableName() {
                    return "dim_base_province";
                }
                
                @Override
                public String getRowKey(TradeProvinceOrderBean bean) {
                    return bean.getProvinceId();
                }
                
                @Override
                public void addDim(TradeProvinceOrderBean bean, JSONObject dimObj) {
                    bean.setProvinceName(dimObj.getString("name"));
                }
            },
            120,
            TimeUnit.SECONDS
        );
        return joinStream;
    }
    
    private void writeToDoris(SingleOutputStreamOperator<TradeProvinceOrderBean> joinStream) {
        joinStream.map(
            new DorisMapFunction<>()
        ).sinkTo(
            FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_PROVINCE_ORDER_WINDOW)
        );
    }
    
    private SingleOutputStreamOperator<TradeProvinceOrderBean> windowAgg(SingleOutputStreamOperator<TradeProvinceOrderBean> duplicationStream) {
        SingleOutputStreamOperator<TradeProvinceOrderBean> windowAggStream = duplicationStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<TradeProvinceOrderBean>() {
                        @Override
                        public long extractTimestamp(TradeProvinceOrderBean element, long recordTimestamp) {
                            return element.getTs();
                        }
                    }
                )
        ).keyBy(
            new KeySelector<TradeProvinceOrderBean, String>() {
                @Override
                public String getKey(TradeProvinceOrderBean value) throws Exception {
                    return value.getProvinceId();
                }
            }
        ).window(
            TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        ).reduce(
            new ReduceFunction<TradeProvinceOrderBean>() {
                @Override
                public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                    value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                    HashSet<String> orderIdSet = new HashSet<>();
                    orderIdSet.addAll(value1.getOrderIdSet());
                    orderIdSet.addAll(value2.getOrderIdSet());
                    value1.setOrderIdSet(orderIdSet);
                    return value1;
                }
            }
            ,
            new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                @Override
                public void process(String string, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> elements, Collector<TradeProvinceOrderBean> out) throws Exception {
                    TradeProvinceOrderBean tradeProvinceOrderBean = elements.iterator().next();
                    
                    tradeProvinceOrderBean.setOrderCount((long) tradeProvinceOrderBean.getOrderIdSet().size());
                    
                    tradeProvinceOrderBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                    tradeProvinceOrderBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                    
                    tradeProvinceOrderBean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));
                    
                    out.collect(tradeProvinceOrderBean);
                }
            }
        );
        return windowAggStream;
    }
    
    private SingleOutputStreamOperator<TradeProvinceOrderBean> duplication(SingleOutputStreamOperator<JSONObject> etlStream) {
        SingleOutputStreamOperator<TradeProvinceOrderBean> duplicationStream = etlStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        }).process(new KeyedProcessFunction<String, JSONObject, TradeProvinceOrderBean>() {
            private ValueState<BigDecimal> valueState;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<BigDecimal> valueStateDesc = new ValueStateDescriptor<>("valueStateDesc", Types.BIG_DEC);
                valueStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30)).build());
                valueState = getRuntimeContext().getState(valueStateDesc);
            }
            
            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TradeProvinceOrderBean>.Context ctx, Collector<TradeProvinceOrderBean> out) throws Exception {
                BigDecimal lastSplitTotalAmount = valueState.value();
                lastSplitTotalAmount = lastSplitTotalAmount == null ? BigDecimal.ZERO : lastSplitTotalAmount;
                
                BigDecimal currSplitTotalAmount = value.getBigDecimal("split_total_amount");
                Set<String> orderIdSet = Collections.singleton(value.getString("order_id"));
                
                TradeProvinceOrderBean provinceOrderBean =
                    TradeProvinceOrderBean.builder()
                        .orderDetailId(value.getString("id"))
                        .provinceId(value.getString("province_id"))
                        .ts(value.getLong("ts"))
                        .orderAmount(currSplitTotalAmount.subtract(lastSplitTotalAmount))
                        .orderIdSet(orderIdSet)
                        .build();
                
                valueState.update(currSplitTotalAmount);
                
                out.collect(provinceOrderBean);
            }
        });
        return duplicationStream;
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> etlStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String Id = jsonObject.getString("id");
                    String orderId = jsonObject.getString("order_id");
                    Long ts = jsonObject.getLong("ts");
                    if (Id != null && orderId != null & ts != null) {
                        jsonObject.put("ts", ts * 1000);
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    log.warn("过滤掉脏数据: " + value);
                }
            }
        });
        return etlStream;
    }
}
