package com.atguigu.gmall.realtime.common.constant;

/**
 * @ClassName Constant
 * @Package com.atguigu.gmall.realtime.common.constant
 * @Author CaiBW
 * @Create 24/01/27 上午 12:01
 * @Description
 */
public class Constant {
    public static final String HDFS_USER_NAME = "atguigu";

    public static final String KAFKA_BROKERS = "linux1:9092,linux2:9092,linux3:9092";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String MYSQL_HOST = "linux1";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "000000";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://linux1:3306?useSSL=false";

    public static final String TABLE_PROCESS_DATABASE = "gmall_config";
    public static final String TABLE_PROCESS_DIM = "table_process_dim";
    public static final String TABLE_PROCESS_DWD = "table_process_dwd";
    
    
    public static final String HBASE_NAMESPACE = "gmall_realtime";
    public static final String HBASE_ZOOKEEPER_QUORUM = "linux1,linux2,linux3";
    
    public static final String DORIS_FENODES = "linux1:7030";
    public static final String DORIS_DATABASE = "gmall_realtime";
    
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PASSWORD = "000000";
    
    public static final String REDIS_HOST = "linux1";
    public static final int REDIS_PORT = 6379;
    public static final int ONE_DAY_SECONDS = 24 * 60 * 60;
    
    
    
    public static final String DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW = "dws_traffic_source_keyword_page_view_window";
    public static final String DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW = "dws_traffic_vc_ch_ar_is_new_page_view_window";
    public static final String DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW = "dws_traffic_home_detail_page_view_window";
    public static final String DWS_USER_USER_REGISTER_WINDOW = "dws_user_user_register_window";
    public static final String DWS_USER_USER_LOGIN_WINDOW = "dws_user_user_login_window";
    public static final String DWS_TRADE_CART_ADD_UU_WINDOW = "dws_trade_cart_add_uu_window";
    public static final String DWS_TRADE_SKU_ORDER_WINDOW = "dws_trade_sku_order_window";
    public static final String DWS_TRADE_PROVINCE_ORDER_WINDOW = "dws_trade_province_order_window";
    
    
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
}
