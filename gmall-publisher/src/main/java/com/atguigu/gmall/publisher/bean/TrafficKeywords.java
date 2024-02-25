package com.atguigu.gmall.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficKeywords {
    // 关键词
    String keyword;
    // 关键词次数
    Integer keywordCount;
}