package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
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
import org.apache.flink.api.common.time.Time;
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
 * @ClassName DwsTrafficHomeDetailPageViewWindowApp
 * @Package com.atguigu.gmall.realtime.dws.app
 * @Author CaiBW
 * @Create 24/02/18 上午 11:45
 * @Description
 */
@Slf4j
public class DwsTrafficHomeDetailPageViewWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindowApp().start(
            10023,
            4,
            "dws_traffic_home_detail_page_view_window_app",
            Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1. 清洗过滤 ，处理成JsonObj格式
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        
        //2. 按照mid分组， 判断是否是首页 ，详情页独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> uvCtStream = uvCtStream(etlStream);
        
        
        //3. 分配水位线
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMarkStream = withWaterMark(uvCtStream);
        
        //4. 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAggStream = windowAgg(withWaterMarkStream);
        
        //5. 写出到Doris
        writeToDoris(windowAggStream);
        
    }
    
    private static void writeToDoris(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAggStream) {
        windowAggStream.map(
            new DorisMapFunction<>()
        ).sinkTo(
            FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW)
        );
    }
    
    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAgg(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMarkStream) {
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAggStream = withWaterMarkStream.windowAll(
            TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        ).reduce(
            new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                @Override
                public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                    //累加 home 和 good_detail的独立访客数
                    value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                    value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                    return value1;
                }
            }
            ,
            new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                @Override
                public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                    TrafficHomeDetailPageViewBean homeDetailPageViewBean = elements.iterator().next();
                    //窗口信息
                    String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                    String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                    //分区信息
                    String currDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                    
                    //补充到Bean对象中
                    homeDetailPageViewBean.setStt(stt);
                    homeDetailPageViewBean.setEdt(edt);
                    homeDetailPageViewBean.setCurDate(currDt);
                    
                    //写出
                    out.collect(homeDetailPageViewBean);
                }
            }
        );
        return windowAggStream;
    }
    
    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMark(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> uvCtStream) {
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMarkStream = uvCtStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
                        @Override
                        public long extractTimestamp(TrafficHomeDetailPageViewBean element, long recordTimestamp) {
                            return element.getTs();
                        }
                    }
                )
        );
        return withWaterMarkStream;
    }
    
    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> uvCtStream(SingleOutputStreamOperator<JSONObject> etlStream) {
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> uvCtStream = etlStream.keyBy(
            new KeySelector<JSONObject, String>() {
                @Override
                public String getKey(JSONObject value) throws Exception {
                    return value.getJSONObject("common").getString("mid");
                }
            }
        ).process(
            new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                
                //声明状态
                private ValueState<String> homeState;
                private ValueState<String> goodDetailState;
                
                //初始化状态
                @Override
                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<String> homeStateDesc = new ValueStateDescriptor<>("homeStateDesc", Types.STRING);
                    homeStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                    homeState = getRuntimeContext().getState(homeStateDesc);
                    
                    ValueStateDescriptor<String> goodDetailDesc = new ValueStateDescriptor<>("goodDetailDesc", Types.STRING);
                    goodDetailDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                    goodDetailState = getRuntimeContext().getState(goodDetailDesc);
                }
                
                @Override
                public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                    // pageId
                    String pageId = value.getJSONObject("page").getString("page_id");
                    
                    // 从状态中提取数据
                    String homeLastDt = homeState.value();
                    String goodDetailLastDt = goodDetailState.value();
                    
                    // 提取当前日期
                    Long ts = value.getLong("ts");
                    String todayDt = DateFormatUtil.tsToDate(ts);
                    
                    Long homeUvCt = 0L;
                    Long goodDetailUvCt = 0L;
                    
                    //判断是否是home的独立访客
                    if ("home".equals(pageId) && !todayDt.equals(homeLastDt)) {
                        homeUvCt = 1L;
                        //更新状态
                        homeState.update(todayDt);
                    }
                    
                    //判断是否是goodDetail的独立访客
                    if ("good_detail".equals(pageId) && !todayDt.equals(goodDetailLastDt)) {
                        goodDetailUvCt = 1L;
                        //更新状态
                        goodDetailState.update(todayDt);
                    }
                    
                    // 如果是home 或者 good_detail的独立访客，将数据写出
                    if (homeUvCt + goodDetailUvCt == 1) {
                        
                        TrafficHomeDetailPageViewBean homeDetailPageViewBean =
                            TrafficHomeDetailPageViewBean.builder()
                                .ts(ts)
                                .homeUvCt(homeUvCt)
                                .goodDetailUvCt(goodDetailUvCt)
                                .build();
                        
                        out.collect(homeDetailPageViewBean);
                    }
                }
            }
        );
        return uvCtStream;
    }
    
    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(
            new FlatMapFunction<String, JSONObject>() {
                @Override
                public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                    try {
                        JSONObject jsonObj = JSON.parseObject(value);
                        JSONObject pageObj = jsonObj.getJSONObject("page");
                        String pageId = pageObj.getString("page_id");
                        String mid = jsonObj.getJSONObject("common").getString("mid");
                        Long ts = jsonObj.getLong("ts");
                        if (("home".equals(pageId) || "good_detail".equals(pageId)) && mid != null && ts != null) {
                            out.collect(jsonObj);
                        }
                    } catch (Exception e) {
                        log.warn("过滤掉脏数据: " + value);
                    }
                }
            }
        );
    }
}
