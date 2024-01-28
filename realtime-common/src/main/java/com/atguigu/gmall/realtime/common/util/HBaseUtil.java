package com.atguigu.gmall.realtime.common.util;


import com.atguigu.gmall.realtime.common.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName HBaseUtil
 * @Package com.atguigu.gmall.realtime.common.util
 * @Author CaiBW
 * @Create 24/01/28 下午 7:22
 * @Description
 */
@Slf4j
public class HBaseUtil {
    /**
     * 获取Connection连接对象
     */
    public static Connection getConnection() throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
        //configuration.set("hbase.zookeeper.property.clientPort" , "2181");

        return ConnectionFactory.createConnection(configuration);
    }

    /**
     * 关闭Connection连接对象
     */
    public static void closeConnection(Connection connection) throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    /**
     * 创建表
     */
    public static void createTable(Connection connection, String namespaceName, String tableName, String... cfs) throws IOException {
        //至少一个列族
        if (cfs == null || cfs.length < 1) {
            log.warn("创建 " + namespaceName + ":" + tableName + " , 至少指定一个列族");
            return;
        }

        //获取Admin对象
        Admin admin = connection.getAdmin();

        TableName tn = TableName.valueOf(namespaceName, tableName);
        //判断表是否已经存在
        if (admin.tableExists(tn)) {
            log.warn("创建 " + namespaceName + ":" + tableName + " , 表已经存在!!!");
            return;
        }

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn);

        //列族
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }

        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        //建表
        admin.createTable(tableDescriptor);
        admin.close();

        log.info("创建 " + namespaceName + ":" + tableName + " , 成功!");
    }

    /**
     * 删除表
     */
    public static void dropTable(Connection connection, String namespaceName, String tableName) throws IOException {
        Admin admin = connection.getAdmin();

        TableName tn = TableName.valueOf(namespaceName, tableName);
        //判断表是否存在
        if (!admin.tableExists(tn)) {
            log.warn("删除 " + namespaceName + ":" + tableName + " , 表不存在!!!");
            return;
        }

        //disable
        admin.disableTable(tn);
        //drop
        admin.deleteTable(tn);

        admin.close();

        log.info("删除 " + namespaceName + ":" + tableName + " , 成功!");
    }

}
