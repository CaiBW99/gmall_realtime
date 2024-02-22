package com.atgguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName DwdInteractionCommentInfoApp
 * @Package PACKAGE_NAME
 * @Author CaiBW
 * @Create 24/01/30 上午 11:40
 * @Description
 */
public class DwdInteractionCommentInfoApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfoApp().start(10012, 4, "dwd_interaction_comment_info_app");
    }

    /**
     * 筛选comment_info数据
     */
    private static void selectCommentInfo(StreamTableEnvironment tableEnv) {
        Table commentInfoTable = tableEnv.sqlQuery(
                "SELECT `data`['id']          id,\n" +
                        "       `data`['user_id']     user_id,\n" +
                        "       `data`['nick_name']   nick_name,\n" +
                        "       `data`['sku_id']      sku_id,\n" +
                        "       `data`['spu_id']      spu_id,\n" +
                        "       `data`['order_id']    order_id,\n" +
                        "       `data`['appraise']    appraise,\n" +
                        "       `data`['comment_txt'] comment_txt,\n" +
                        "       `data`['create_time'] create_time,\n" +
                        "       `pt`,\n" +
                        "       ts\n" +
                        "FROM topic_db\n" +
                        "WHERE `database` = 'gmall'\n" +
                        "  AND `table` = 'comment_info'\n" +
                        "  AND `type` = 'insert'"
        );
        tableEnv.createTemporaryView("comment_info", commentInfoTable);
    }

    /**
     * join
     */
    private static Table join(StreamTableEnvironment tableEnv) {
        Table joinTable = tableEnv.sqlQuery(
                "SELECT ci.id,\n" +
                        "       ci.user_id,\n" +
                        "       ci.nick_name,\n" +
                        "       ci.sku_id,\n" +
                        "       ci.spu_id,\n" +
                        "       ci.order_id,\n" +
                        "       ci.appraise      appraise_id,\n" +
                        "       bd.info.dic_name appraise_name,\n" +
                        "       ci.comment_txt,\n" +
                        "       ci.create_time,\n" +
                        "       ci.ts\n" +
                        "FROM comment_info ci\n" +
                        "    JOIN base_dic FOR SYSTEM_TIME AS OF ci.pt AS bd\n" +
                        "ON ci.appraise = bd.dic_code"
        );
        return joinTable;
    }

    /**
     * 写出到kafka
     */
    private static void writeToKafka(StreamTableEnvironment tableEnv, Table joinTable) {
        tableEnv.executeSql(
                "CREATE TABLE " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + "(\n" +
                        "    id            STRING,\n" +
                        "    user_id       STRING,\n" +
                        "    nick_name     STRING,\n" +
                        "    sku_id        STRING,\n" +
                        "    spu_id        STRING,\n" +
                        "    order_id      STRING,\n" +
                        "    appraise_id   STRING,\n" +
                        "    appraise_name STRING,\n" +
                        "    comment_txt   STRING,\n" +
                        "    create_time   STRING,\n" +
                        "    ts            BIGINT\n" +
                        ")" + FlinkSQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)
        );
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // TODO 1.读取topic_db数据
        readOdsTopicDb(tableEnv, groupId);

        // TODO 2.筛选comment_info数据
        selectCommentInfo(tableEnv);

        // TODO 3.读取base_dic表
        readBaseDic(tableEnv);

        // TODO 4.join
        Table joinTable = join(tableEnv);

        // TODO 5.写出到kafka
        writeToKafka(tableEnv, joinTable);

    }
}
