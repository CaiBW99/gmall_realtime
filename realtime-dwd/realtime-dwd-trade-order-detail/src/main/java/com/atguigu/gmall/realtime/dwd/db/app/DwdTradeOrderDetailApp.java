package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @ClassName DwdTradeOrderDetailApp
 * @Package com.atguigu.gmall.realtime.dwd.db.app
 * @Author CaiBW
 * @Create 24/01/31 上午 11:44
 * @Description 交易域下单事务事实表
 * <p>关联订单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表的insert操作，形成下单明细表，写入Kafka对应主题
 */
public class DwdTradeOrderDetailApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetailApp().start(10014, 4, "dwd_trade_order_detail_app");
    }
    
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 设置乱序5s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        
        //TODO 1. 读取ods  topic_db的数据
        // 从ods读取，直接调用BaseSQLApp方法
        readOdsTopicDb(tableEnv, groupId);
        
        //TODO 2. 筛选订单明细数据
        filterOrderDetail(tableEnv);
        
        //TODO 3. 筛选订单表数据
        filterOrderInfo(tableEnv);
        
        //TODO 4. 筛选订单明细活动关联表
        filterOrderDetailActivity(tableEnv);
        
        //TODO 5. 筛选订单明细优惠券关联表
        filterOrderDetailCoupon(tableEnv);
        
        //TODO 6. join
        Table joinTable = join(tableEnv);
        
        //TODO 7. 写出到kafka
        writeToKafka(tableEnv, joinTable);
    }
    
    /**
     * 写出到kafka
     */
    private void writeToKafka(StreamTableEnvironment tableEnv, Table joinTable) {
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
                "    coupon_id             STRING,\n" +
                "    PRIMARY KEY ( id ) NOT ENFORCED\n" +
                ")" + FlinkSQLUtil.getUpsertKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );
        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();
    }
    
    /**
     * join
     */
    private Table join(StreamTableEnvironment tableEnv) {
        Table joinTable = tableEnv.sqlQuery(
            "SELECT od.id,\n" +
                "       od.order_id,\n" +
                "       od.sku_id,\n" +
                "       od.sku_name,\n" +
                "       od.order_price,\n" +
                "       od.sku_num,\n" +
                "       od.create_time,\n" +
                "       od.split_total_amount,\n" +
                "       od.split_activity_amount,\n" +
                "       od.split_coupon_amount,\n" +
                "       od.ts,\n" +
                "       oi.user_id           user_id,\n" +
                "       oi.province_id       province_id,\n" +
                "       oda.activity_id      activity_id,\n" +
                "       oda.activity_rule_id activity_rule_id,\n" +
                "       odc.coupon_id        coupon_id\n" +
                "FROM order_detail od\n" +
                "    JOIN order_info oi\n" +
                "         ON od.order_id = oi.id\n" +
                "    LEFT JOIN order_detail_activity oda\n" +
                "              ON od.id = oda.order_detail_id\n" +
                "    LEFT JOIN order_detail_coupon odc\n" +
                "              ON od.id = odc.order_detail_id"
        );
        return joinTable;
    }
    
    /**
     * 筛选订单明细优惠券关联表
     */
    private void filterOrderDetailCoupon(StreamTableEnvironment tableEnv) {
        Table orderDetailCouponTable = tableEnv.sqlQuery(
            "SELECT `data`['order_detail_id'] order_detail_id,\n" +
                "       `data`['coupon_id']       coupon_id\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "  AND `table` = 'order_detail_coupon'\n" +
                "  AND `type` = 'insert'"
        );
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCouponTable);
    }
    
    /**
     * 筛选订单明细活动关联表
     */
    private void filterOrderDetailActivity(StreamTableEnvironment tableEnv) {
        Table orderDetailActivityTable = tableEnv.sqlQuery(
            "SELECT `data`['order_detail_id']  order_detail_id,\n" +
                "       `data`['activity_id']      activity_id,\n" +
                "       `data`['activity_rule_id'] activity_rule_id\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "  AND `table` = 'order_detail_activity'\n" +
                "  AND `type` = 'insert'"
        );
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivityTable);
    }
    
    /**
     * 筛选订单表数据
     */
    private void filterOrderInfo(StreamTableEnvironment tableEnv) {
        Table orderInfoTable = tableEnv.sqlQuery(
            "SELECT `data`['id']          id,\n" +
                "       `data`['user_id']     user_id,\n" +
                "       `data`['province_id'] province_id\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "  AND `table` = 'order_info'\n" +
                "  AND `type` = 'insert'"
        );
        tableEnv.createTemporaryView("order_info", orderInfoTable);
    }
    
    /**
     * 筛选订单明细数据
     */
    private void filterOrderDetail(StreamTableEnvironment tableEnv) {
        Table orderDetailTable = tableEnv.sqlQuery(
            "SELECT `data`['id']                    id,\n" +
                "       `data`['order_id']              order_id,\n" +
                "       `data`['sku_id']                sku_id,\n" +
                "       `data`['sku_name']              sku_name,\n" +
                "       `data`['order_price']           order_price,\n" +
                "       `data`['sku_num']               sku_num,\n" +
                "       `data`['create_time']           create_time,\n" +
                "       `data`['split_total_amount']    split_total_amount,\n" +
                "       `data`['split_activity_amount'] split_activity_amount,\n" +
                "       `data`['split_coupon_amount']   split_coupon_amount,\n" +
                "       `ts`\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "  AND `table` = 'order_detail'\n" +
                "  AND `type` = 'insert'"
        );
        tableEnv.createTemporaryView("order_detail", orderDetailTable);
    }
}
