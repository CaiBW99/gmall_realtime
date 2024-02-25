package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.TrafficKeywords;

import java.util.List;

/**
 * @ClassName TrafficKeywordsService
 * @Package com.atguigu.gmall.publisher.service.impl
 * @Author CaiBW
 * @Create 24/02/25 下午 5:00
 * @Description
 */
public interface TrafficKeywordsService {
    List<TrafficKeywords> getKeywords(Integer date);
}
