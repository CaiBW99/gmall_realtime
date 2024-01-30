package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName DwdTradeCartAddApp
 * @Package PACKAGE_NAME
 * @Author CaiBW
 * @Create 24/01/30 下午 2:25
 * @Description
 */
public class DwdTradeCartAddApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAddApp().start(10013, 4, "dwd_trade_cart_add_app");
    }

    /**
     * 筛选加购数据
     */
    private static Table selectCartAdd(StreamTableEnvironment tableEnv) {
        Table cartAddTable = tableEnv.sqlQuery("SELECT `data`['id']                                                             id,\n" + "       `data`['user_id']                                                        user_id,\n" + "       `data`['sku_id']                                                         sku_id,\n" + "       `data`['cart_price']                                                     cart_price,\n" + "       IF( `type` = 'insert', CAST( `data`['sku_num'] AS INT ),\n" + "           CAST( `data`['sku_num'] AS INT ) - CAST( `old`['sku_num'] AS INT ) ) sku_num,\n" + "       `data`['sku_name']                                                       sku_name,\n" + "       `data`['is_checked']                                                     is_checked,\n" + "       `data`['create_time']                                                    create_time,\n" + "       `data`['operate_time']                                                   operate_time,\n" + "       `data`['is_ordered']                                                     is_ordered,\n" + "       `data`['order_time']                                                     order_time,\n" + "       `data`['source_type']                                                    source_type,\n" + "       `data`['source_id']                                                      source_id,\n" + "       ts\n" + "FROM topic_db\n" + "WHERE `database` = 'gmall'\n" + "  AND `table` = 'cart_info'\n" + "  AND (`type` = 'insert'\n" + "    OR\n" + "       (`type` = 'update' AND `old`['sku_num'] IS NOT NULL AND\n" + "        CAST( `data`['sku_num'] AS INT ) > CAST( `old`['sku_num'] AS INT ))\n" + "    )");
        return cartAddTable;
    }

    /**
     * 写出到kafka
     */
    private static void writeToKafka(StreamTableEnvironment tableEnv, Table cartAddTable) {
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" + "    id           STRING,\n" + "    user_id      STRING,\n" + "    sku_id       STRING,\n" + "    cart_price   STRING,\n" + "    sku_num      INT,\n" + "    sku_name     STRING,\n" + "    is_checked   STRING,\n" + "    create_time  STRING,\n" + "    operate_time STRING,\n" + "    is_ordered   STRING,\n" + "    order_time   STRING,\n" + "    source_type  STRING,\n" + "    source_id    STRING,\n" + "    ts           BIGINT\n" + ")" + FlinkSQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        cartAddTable.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // TODO 1.读取ods中topic_db数据
        readOdsTopicDb(tableEnv, groupId);
        // TODO 2.筛选出加购数据
        Table cartAddTable = selectCartAdd(tableEnv);
        // TODO 3.写出到kafka
        writeToKafka(tableEnv, cartAddTable);

    }
}

