package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName DwdTradeOrderRefundApp
 * @Package com.atguigu.gmall.realtime.dwd.db.app
 * @Author CaiBW
 * @Create 24/02/18 上午 11:37
 * @Description 交易域退单事务事实表 (练习)
 */
public class DwdTradeOrderRefundApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderRefundApp().start(
            10017,
            4,
            Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }
    
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //TODO 1.读取topic_db数据, 读取字典表
        readOdsTopicDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
        readBaseDic(tableEnv);
        
        //TODO 2.过滤退单表数据 order_refund_info insert
        Table orderRefundInfo = tableEnv.sqlQuery(
            "SELECT data['id']                 id,\n" +
                "       data['user_id']            user_id,\n" +
                "       data['order_id']           order_id,\n" +
                "       data['sku_id']             sku_id,\n" +
                "       data['refund_type']        refund_type,\n" +
                "       data['refund_num']         refund_num,\n" +
                "       data['refund_amount']      refund_amount,\n" +
                "       data['refund_reason_type'] refund_reason_type,\n" +
                "       data['refund_reason_txt']  refund_reason_txt,\n" +
                "       data['create_time']        create_time,\n" +
                "       pt,\n" +
                "       ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "  AND `table` = 'order_refund_info'\n" +
                "  AND `type` = 'insert'"
        );
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
        
        //TODO 3.过滤订单表中的退单数据: order_info  update
        Table orderInfo = tableEnv.sqlQuery(
            "select \n" +
                "data['id'] id,\n" +
                "data['province_id'] province_id,\n" +
                "`old` \n" +
                "from topic_db \n" +
                "where `database`='gmall' \n" +
                "and `table`='order_info' \n" +
                "and `type`='update'\n" +
                "and `old`['order_status'] is not null \n" +
                "and `data`['order_status']='1005'"
        );
        tableEnv.createTemporaryView("order_info", orderInfo);
        
        //TODO 4.join
        Table result = tableEnv.sqlQuery(
            "SELECT ri.id,\n" +
                "       ri.user_id,\n" +
                "       ri.order_id,\n" +
                "       ri.sku_id,\n" +
                "       oi.province_id,\n" +
                "       DATE_FORMAT( ri.create_time, 'yyyy-MM-dd' ) date_id,\n" +
                "       ri.create_time,\n" +
                "       ri.refund_type,\n" +
                "       dic1.info.dic_name,\n" +
                "       ri.refund_reason_type,\n" +
                "       dic2.info.dic_name,\n" +
                "       ri.refund_reason_txt,\n" +
                "       ri.refund_num,\n" +
                "       ri.refund_amount,\n" +
                "       ri.ts\n" +
                "FROM order_refund_info ri\n" +
                "    JOIN order_info oi\n" +
                "         ON ri.order_id = oi.id\n" +
                "    JOIN base_dic FOR SYSTEM_TIME AS OF ri.pt AS dic1\n" +
                "         ON ri.refund_type=dic1.dic_code\n" +
                "    JOIN base_dic FOR SYSTEM_TIME AS OF ri.pt AS dic2\n" +
                "         ON ri.refund_reason_type=dic2.dic_code"
        );
        
        //TODO 5.写出到 kafka
        tableEnv.executeSql(
            "CREATE TABLE dwd_trade_order_refund(\n" +
                "    id STRING,\n" +
                "    user_id STRING,\n" +
                "    order_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    province_id STRING,\n" +
                "    date_id STRING,\n" +
                "    create_time STRING,\n" +
                "    refund_type_code STRING,\n" +
                "    refund_type_name STRING,\n" +
                "    refund_reason_type_code STRING,\n" +
                "    refund_reason_type_name STRING,\n" +
                "    refund_reason_txt STRING,\n" +
                "    refund_num STRING,\n" +
                "    refund_amount STRING,\n" +
                "    ts BIGINT\n" +
                ")" + FlinkSQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND)
        );
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }
}
