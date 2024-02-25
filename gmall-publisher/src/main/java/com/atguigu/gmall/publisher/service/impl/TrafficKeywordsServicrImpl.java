package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.bean.TrafficKeywords;
import com.atguigu.gmall.publisher.mapper.TrafficKeywordsMapper;
import com.atguigu.gmall.publisher.service.TrafficKeywordsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @ClassName TrafficKeywordsServicrImpl
 * @Package com.atguigu.gmall.publisher.service.impl
 * @Author CaiBW
 * @Create 24/02/25 下午 5:01
 * @Description
 */
@Service
public class TrafficKeywordsServicrImpl implements TrafficKeywordsService {
    @Autowired
    TrafficKeywordsMapper trafficKeywordsMapper;
    
    @Override
    public List<TrafficKeywords> getKeywords(Integer date) {
        return trafficKeywordsMapper.selectKeywords(date);
    }
}
