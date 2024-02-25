package com.atguigu.gmall.publisher.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
    
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    
    /**
     * 获取当天日期 ， 格式为 20250225
     *
     * @return
     */
    public static Integer now() {
        Date date = new Date();
        String dateStr = sdf.format(date);
        
        return Integer.valueOf(dateStr);
    }
    
    public static void main(String[] args) {
        System.out.println(now());
    }
}