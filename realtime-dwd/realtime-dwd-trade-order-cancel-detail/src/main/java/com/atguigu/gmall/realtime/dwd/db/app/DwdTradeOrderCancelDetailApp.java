package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @ClassName DwdTradeOrderCancelDetail
 * @Package com.atguigu.gmall.realtime.dwd.db.app
 * @Author CaiBW
 * @Create 24/02/01 下午 2:03
 * @Description 交易域取消订单事务事实表（练习）
 * <p>关联筛选订单明细表、取消订单数据、订单明细活动关联表、订单明细优惠券关联表
 */
public class DwdTradeOrderCancelDetailApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetailApp().start(10015, 4, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
    
    /**
     * @param tableEnv
     * @param env
     * @param groupId
     */
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 迟到数据5s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 5));
        
        // TODO 1.读取ods topic_db
        readOdsTopicDb(tableEnv, groupId);
        
        // TODO 2.读取下单事务事实表
        tableEnv.executeSql(
            "CREATE TABLE " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + "(\n" +
                "    id                    STRING,\n" +
                "    order_id              STRING,\n" +
                "    sku_id                STRING,\n" +
                "    sku_name              STRING,\n" +
                "    order_price           STRING,\n" +
                "    sku_num               STRING,\n" +
                "    create_time           STRING,\n" +
                "    split_total_amount    STRING,\n" +
                "    split_activity_amount STRING,\n" +
                "    split_coupon_amount   STRING,\n" +
                "    ts                    BIGINT,\n" +
                "    user_id               STRING,\n" +
                "    province_id           STRING,\n" +
                "    activity_id           STRING,\n" +
                "    activity_rule_id      STRING,\n" +
                "    coupon_id             STRING\n" +
                ")" + FlinkSQLUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL)
        );
        
        //tableEnv.sqlQuery("select * from dwd_trade_order_detail").execute().print();
        
        
        // TODO 3.读取订单取消数据
        Table orderCancel = tableEnv.sqlQuery(
            "select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] operate_time, " +
                " `ts` " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status']='1001' " +
                "and `data`['order_status']='1005' "
        );
        tableEnv.createTemporaryView("order_cancel", orderCancel);
        //tableEnv.sqlQuery("select * from order_cancel").execute().print();
        
        // TODO 4.join
        Table result = tableEnv.sqlQuery(
            "select od.id, \n" +
                "od.order_id, \n" +
                "od.user_id, \n" +
                "od.sku_id, \n" +
                "od.sku_name, \n" +
                "od.province_id, \n" +
                "od.activity_id, \n" +
                "od.activity_rule_id, \n" +
                "od.coupon_id, \n" +
                "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id, \n" +
                "oc.operate_time, \n" +
                "od.sku_num, \n" +
                "od.split_activity_amount, \n" +
                "od.split_coupon_amount, \n" +
                "od.split_total_amount, \n" +
                "oc.ts \n" +
                "from dwd_trade_order_detail od \n" +
                "join order_cancel oc \n" +
                "on od.order_id=oc.id "
        );
        //result.execute().print();
        
        // TODO 5.写出
        tableEnv.executeSql(
            "CREATE TABLE " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + "(\n" +
                "    id STRING,\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    sku_name STRING,\n" +
                "    province_id STRING,\n" +
                "    activity_id STRING,\n" +
                "    activity_rule_id STRING,\n" +
                "    coupon_id STRING,\n" +
                "    date_id STRING,\n" +
                "    cancel_time STRING,\n" +
                "    sku_num STRING,\n" +
                "    split_activity_amount STRING,\n" +
                "    split_coupon_amount STRING,\n" +
                "    split_total_amount STRING,\n" +
                "    ts BIGINT\n" +
                ")" + FlinkSQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL)
        );
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
        
    }
}
