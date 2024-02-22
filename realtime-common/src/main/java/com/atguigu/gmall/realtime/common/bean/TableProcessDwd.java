package com.atguigu.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName TableProcessDwd
 * @Package com.atguigu.gmall.realtime.common.bean
 * @Author CaiBW
 * @Create 24/01/31 下午 7:23
 * @Description
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd {
    // 来源表名
    String sourceTable;
    
    // 来源类型
    String sourceType;
    
    // 目标表名
    String sinkTable;
    
    // 输出字段
    String sinkColumns;
    
    // 配置表操作类型
    String op;
}
