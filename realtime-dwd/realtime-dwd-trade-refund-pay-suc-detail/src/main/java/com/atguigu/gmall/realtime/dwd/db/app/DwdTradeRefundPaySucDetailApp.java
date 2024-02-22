package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName DwdTradeRefundPaySucDetailApp
 * @Package com.atguigu.gmall.realtime.dwd.db.app
 * @Author CaiBW
 * @Create 24/02/18 上午 11:38
 * @Description 交易域退款成功事务事实表（练习）
 */
public class DwdTradeRefundPaySucDetailApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetailApp().start(10018, 4, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }
    
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //TODO 1.读取topic_db,读取字典表
        readOdsTopicDb(tableEnv, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
        readBaseDic(tableEnv);
        
        //TODO 2.过滤退款成功表数据
        Table refundPayment = tableEnv.sqlQuery(
            "SELECT data['id']            id,\n" +
                "       data['order_id']      order_id,\n" +
                "       data['sku_id']        sku_id,\n" +
                "       data['payment_type']  payment_type,\n" +
                "       data['callback_time'] callback_time,\n" +
                "       data['total_amount']  total_amount,\n" +
                "       pt,\n" +
                "       ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "  AND `table` = 'refund_payment'\n" +
                "  AND `type` = 'update'\n" +
                "  AND `old`['refund_status'] IS NOT NULL\n" +
                "  AND `data`['refund_status'] = '1602'"
        );
        tableEnv.createTemporaryView("refund_payment", refundPayment);
        
        //TODO 3.过滤退单表中的退单成功数据
        Table orderRefundInfo = tableEnv.sqlQuery(
            "SELECT data['order_id']   order_id,\n" +
                "       data['sku_id']     sku_id,\n" +
                "       data['refund_num'] refund_num\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "  AND `table` = 'order_refund_info'\n" +
                "  AND `type` = 'update'\n" +
                "  AND `old`['refund_status'] IS NOT NULL\n" +
                "  AND `data`['refund_status'] = '0705'"
        );
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
        
        //TODO 4.过滤订单表中的退款成功数据
        Table orderInfo = tableEnv.sqlQuery(
            "SELECT data['id']          id,\n" +
                "       data['user_id']     user_id,\n" +
                "       data['province_id'] province_id\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "  AND `table` = 'order_info'\n" +
                "  AND `type` = 'update'\n" +
                "  AND `old`['order_status'] IS NOT NULL\n" +
                "  AND `data`['order_status'] = '1006'"
        );
        tableEnv.createTemporaryView("order_info", orderInfo);
        
        //TODO 5.join
        Table result = tableEnv.sqlQuery(
            "SELECT rp.id,\n" +
                "       oi.user_id,\n" +
                "       rp.order_id,\n" +
                "       rp.sku_id,\n" +
                "       oi.province_id,\n" +
                "       rp.payment_type,\n" +
                "       dic.info.dic_name                             payment_type_name,\n" +
                "       DATE_FORMAT( rp.callback_time, 'yyyy-MM-dd' ) date_id,\n" +
                "       rp.callback_time,\n" +
                "       ori.refund_num,\n" +
                "       rp.total_amount,\n" +
                "       rp.ts\n" +
                "FROM refund_payment rp\n" +
                "    JOIN order_refund_info ori\n" +
                "         ON rp.order_id = ori.order_id AND rp.sku_id = ori.sku_id\n" +
                "    JOIN order_info oi\n" +
                "         ON rp.order_id = oi.id\n" +
                "    JOIN base_dic FOR SYSTEM_TIME AS OF rp.pt AS dic\n" +
                "ON rp.payment_type=dic.dic_code"
        );
        
        //TODO 6.写入到Kafka
        tableEnv.executeSql(
            "CREATE TABLE " + Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS + "(\n" +
                "    id STRING,\n" +
                "    user_id STRING,\n" +
                "    order_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    province_id STRING,\n" +
                "    payment_type_code STRING,\n" +
                "    payment_type_name STRING,\n" +
                "    date_id STRING,\n" +
                "    callback_time STRING,\n" +
                "    refund_num STRING,\n" +
                "    refund_amount STRING,\n" +
                "    ts BIGINT\n" +
                ")" + FlinkSQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS)
        );
        result.insertInto(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS).execute();
    }
}
