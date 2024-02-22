package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
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
import java.util.concurrent.TimeUnit;

@Slf4j
public class DwsTradeSkuOrderWindowASyncCacheApp extends BaseApp {
    
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowASyncCacheApp().start(
            10029,
            4,
            "dws_trade_sku_order_window_sync_cache_app",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //stream.print() ;
        //1. 过滤清洗， 转换成JsonObject结构
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        
        //etlStream.print() ;
        
        //2. 去重
        // 方案1:  状态 + 定时器
        // 方案2:  状态 + 抵消
        // +I
        // -D  null, 已过滤
        // +I
        
        // +I  o1  100  200  300   null   null
        // -D
        // +I  o1  100  200  300   a1   null  (之前发: o1  -100  -200  -300   null   null)
        // +I 优化:  o1  0(100 - 100)  0(200-200)  0(300 - 300 )   a1   null
        // -D
        // +I  o1  100  200  300   a1   c1    (之前发: o1  -100  -200  -300   null   null)
        // +I 优化: o1  0(100 - 100)  0(200-200)  0(300 - 300 )   a1   c1
        
        SingleOutputStreamOperator<TradeSkuOrderBean> duplicateStream = duplicate(etlStream);
        
        //duplicateStream.print() ;
        
        //3. 分配水位线 ， 开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> windowAggStream = windowAgg(duplicateStream);
        
        //windowAggStream.print() ;
        
        
        //4. 维度关联
        // DIM: HBase
        // 需要关联的表 级 关联顺序:
        // dim_sku_info
        // dim_spu_info
        // dim_base_trademark
        // dim_base_category3
        // dim_base_category2
        // dim_base_category1
        
        SingleOutputStreamOperator<TradeSkuOrderBean> joinDimStream =
            joinDimAsync(windowAggStream);
        
        joinDimStream.print();
        
        //5. 写出到Doris
        writeToDoris(joinDimStream);
        
    }
    
    private static void writeToDoris(SingleOutputStreamOperator<TradeSkuOrderBean> joinDimStream) {
        joinDimStream.map(
            new DorisMapFunction<>()
        ).sinkTo(
            FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_SKU_ORDER_WINDOW)
        );
    }
    
    
    private static SingleOutputStreamOperator<TradeSkuOrderBean> joinDimAsync(SingleOutputStreamOperator<TradeSkuOrderBean> windowAggStream) {
        
        // 关联 dim_sku_info
        
        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfoStream = AsyncDataStream.unorderedWait(windowAggStream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_sku_info";
            }
            
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getSkuId();
            }
            
            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setSpuId(dimObj.getString("spu_id"));
                bean.setTrademarkId(dimObj.getString("tm_id"));
                bean.setCategory3Id(dimObj.getString("category3_id"));
                
            }
        }, 120, TimeUnit.SECONDS);
        
        
        // 关联 dim_spu_info
        SingleOutputStreamOperator<TradeSkuOrderBean> spuInfoStream = AsyncDataStream.unorderedWait(skuInfoStream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_spu_info";
            }
            
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getSpuId();
            }
            
            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setSpuName(dimObj.getString("spu_name"));
            }
        }, 120, TimeUnit.SECONDS);
        
        // 关联 dim_base_trademark
        SingleOutputStreamOperator<TradeSkuOrderBean> baseTrademarkStream = AsyncDataStream.unorderedWait(spuInfoStream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_base_trademark";
            }
            
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getTrademarkId();
            }
            
            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setTrademarkName(dimObj.getString("tm_name"));
            }
        }, 120, TimeUnit.SECONDS);
        
        
        // 关联 dim_base_category3
        SingleOutputStreamOperator<TradeSkuOrderBean> baseCategory3Stream = AsyncDataStream.unorderedWait(baseTrademarkStream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_base_category3";
            }
            
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory3Id();
            }
            
            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setCategory3Name(dimObj.getString("name"));
                bean.setCategory2Id(dimObj.getString("category2_id"));
            }
        }, 120, TimeUnit.SECONDS);
        
        
        //关联 dim_base_category2
        
        SingleOutputStreamOperator<TradeSkuOrderBean> baseCategory2Stream = AsyncDataStream.unorderedWait(baseCategory3Stream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_base_category2";
            }
            
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory2Id();
            }
            
            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setCategory2Name(dimObj.getString("name"));
                bean.setCategory1Id(dimObj.getString("category1_id"));
            }
            
            
        }, 120, TimeUnit.SECONDS);
        
        
        //关联 dim_base_category1
        SingleOutputStreamOperator<TradeSkuOrderBean> baseCategory1Stream = AsyncDataStream.unorderedWait(baseCategory2Stream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_base_category1";
            }
            
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory1Id();
            }
            
            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setCategory1Name(dimObj.getString("name"));
            }
        }, 120, TimeUnit.SECONDS);
        
        return baseCategory1Stream;
        
    }
    
    
    private static SingleOutputStreamOperator<TradeSkuOrderBean> windowAgg(SingleOutputStreamOperator<TradeSkuOrderBean> duplicateStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> windowAggStream = duplicateStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                        @Override
                        public long extractTimestamp(TradeSkuOrderBean element, long recordTimestamp) {
                            return element.getTs();
                        }
                    }
                )
        ).keyBy(
            new KeySelector<TradeSkuOrderBean, String>() {
                @Override
                public String getKey(TradeSkuOrderBean value) throws Exception {
                    return value.getSkuId();
                }
            }
        ).window(
            TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        ).reduce(
            new ReduceFunction<TradeSkuOrderBean>() {
                @Override
                public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                    
                    value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                    value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                    value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                    value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                    
                    return value1;
                }
            },
            new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                @Override
                public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                    TradeSkuOrderBean tradeSkuOrderBean = elements.iterator().next();
                    //补充窗口信息
                    tradeSkuOrderBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                    tradeSkuOrderBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                    //补充分区信息
                    tradeSkuOrderBean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));
                    
                    //写出
                    out.collect(tradeSkuOrderBean);
                }
            }
        );
        return windowAggStream;
    }
    
    private static SingleOutputStreamOperator<TradeSkuOrderBean> duplicate(SingleOutputStreamOperator<JSONObject> etlStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> duplicateStream = etlStream.keyBy(
            new KeySelector<JSONObject, String>() {
                @Override
                public String getKey(JSONObject value) throws Exception {
                    return value.getString("id");
                }
            }
        ).process(
            new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
                
                //声明状态， 维护未来需要聚合的度量值
                private MapState<String, BigDecimal> mapState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    MapStateDescriptor<String, BigDecimal> mapStateDesc
                        = new MapStateDescriptor<>("mapStateDesc", Types.STRING, Types.BIG_DEC);
                    //状态TTL
                    mapStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30)).build());
                    mapState = getRuntimeContext().getMapState(mapStateDesc);
                }
                
                @Override
                public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {
                    // 每条数据到达后，需要从状态中取出对应的值， 进行修正度量值， 完成抵消
                    BigDecimal originalAmount = mapState.get("originalAmount");
                    BigDecimal activityReduceAmount = mapState.get("activityReduceAmount");
                    BigDecimal couponReduceAmount = mapState.get("couponReduceAmount");
                    BigDecimal orderAmount = mapState.get("orderAmount");

                        /*if(originalAmount != null && originalAmount.longValue() > 0  ){
                            System.out.println("================有撤回情况: " + jsonObj.getString("id"));
                        }*/
                    
                    //判断状态值，如果为空，置为0
                    originalAmount = originalAmount == null ? BigDecimal.ZERO : originalAmount;
                    activityReduceAmount = activityReduceAmount == null ? BigDecimal.ZERO : activityReduceAmount;
                    couponReduceAmount = couponReduceAmount == null ? BigDecimal.ZERO : couponReduceAmount;
                    orderAmount = orderAmount == null ? BigDecimal.ZERO : orderAmount;
                    
                    //计算当前的orderAmount
                    BigDecimal currOrderAmount =
                        jsonObj.getBigDecimal("order_price").multiply(jsonObj.getBigDecimal("sku_num"));
                    BigDecimal currSplitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                    BigDecimal currSplitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                    BigDecimal currSplitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                    //封装成TradeSkuOrderBean ， 并修正度量值
                    TradeSkuOrderBean tradeSkuOrderBean =
                        TradeSkuOrderBean.builder()
                            .skuId(jsonObj.getString("sku_id"))
                            .skuName(jsonObj.getString("sku_name"))
                            .ts(jsonObj.getLong("ts"))
                            .orderDetailId(jsonObj.getString("id"))
                            .originalAmount(currSplitTotalAmount.subtract(originalAmount))
                            .activityReduceAmount(currSplitActivityAmount.subtract(activityReduceAmount))
                            .couponReduceAmount(currSplitCouponAmount.subtract(couponReduceAmount))
                            .orderAmount(currOrderAmount.subtract(orderAmount))
                            .build();
                    
                    // 将当前数据中的度量值更新到状态中
                    mapState.put("originalAmount", currSplitTotalAmount);
                    mapState.put("activityReduceAmount", currSplitActivityAmount);
                    mapState.put("couponReduceAmount", currSplitCouponAmount);
                    mapState.put("orderAmount", currOrderAmount);
                    
                    //写出
                    out.collect(tradeSkuOrderBean);
                    
                }
            }
        );
        return duplicateStream;
    }
    
    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> etlStream = stream.flatMap(
            new FlatMapFunction<String, JSONObject>() {
                @Override
                public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                    try {
                        if (value != null) {
                            JSONObject jsonObj = JSONObject.parseObject(value);
                            String orderDetailId = jsonObj.getString("id");
                            String skuId = jsonObj.getString("sku_id");
                            Long ts = jsonObj.getLong("ts");
                            if (orderDetailId != null && skuId != null && ts != null) {
                                //将ts处理成毫秒级
                                jsonObj.put("ts", ts * 1000);
                                out.collect(jsonObj);
                            }
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