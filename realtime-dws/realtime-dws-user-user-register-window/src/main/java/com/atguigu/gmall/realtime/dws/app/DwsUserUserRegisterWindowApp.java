package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName DwsUserUserRegisterWindowApp
 * @Package com.atguigu.gmall.realtime.dws.app
 * @Author CaiBW
 * @Create 24/02/18 下午 3:44
 * @Description
 */
@Slf4j
public class DwsUserUserRegisterWindowApp extends BaseApp {
    
    public static void main(String[] args) {
        
        new DwsUserUserRegisterWindowApp().start(
            10025,
            4,
            "dws_user_user_register_window_app",
            Constant.TOPIC_DWD_USER_REGISTER
        );
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1. 清洗过滤， 转换成UserRegisterBean格式
        SingleOutputStreamOperator<UserRegisterBean> etlStream = etl(stream);
        
        //etlStream.print() ;
        
        //2.分配水位线 , 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> windowStream = windowAgg(etlStream);
        
        //3. 写出到Doris
        writeToDoris(windowStream);
        
    }
    
    private static void writeToDoris(SingleOutputStreamOperator<UserRegisterBean> windowStream) {
        windowStream.map(
            new DorisMapFunction<>()
        ).sinkTo(
            FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_REGISTER_WINDOW)
        );
    }
    
    private static SingleOutputStreamOperator<UserRegisterBean> windowAgg(SingleOutputStreamOperator<UserRegisterBean> etlStream) {
        SingleOutputStreamOperator<UserRegisterBean> windowStream = etlStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<UserRegisterBean>() {
                        @Override
                        public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                            return element.getTs();
                        }
                    }
                )
        ).windowAll(
            TumblingEventTimeWindows.of(Time.seconds(10))
        ).reduce(
            new ReduceFunction<UserRegisterBean>() {
                @Override
                public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                    value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                    return value1;
                }
            }
            ,
            new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                @Override
                public void process(ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>.Context context, Iterable<UserRegisterBean> elements, Collector<UserRegisterBean> out) throws Exception {
                    UserRegisterBean userRegisterBean = elements.iterator().next();
                    
                    //补充窗口信息
                    userRegisterBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                    userRegisterBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                    userRegisterBean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));
                    
                    out.collect(userRegisterBean);
                }
            }
        );
        
        //windowStream.print() ;
        return windowStream;
    }
    
    private static SingleOutputStreamOperator<UserRegisterBean> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<UserRegisterBean> etlStream = stream.flatMap(
            new FlatMapFunction<String, UserRegisterBean>() {
                @Override
                public void flatMap(String value, Collector<UserRegisterBean> out) throws Exception {
                    try {
                        JSONObject jsonObj = JSON.parseObject(value);
                        UserRegisterBean userRegisterBean = UserRegisterBean.builder()
                            .registerCt(1L)
                            .ts(DateFormatUtil.dateTimeToTs(jsonObj.getString("create_time")))
                            .build();
                        out.collect(userRegisterBean);
                        
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.warn("过滤掉脏数据: " + value);
                    }
                }
            }
        );
        return etlStream;
    }
}
