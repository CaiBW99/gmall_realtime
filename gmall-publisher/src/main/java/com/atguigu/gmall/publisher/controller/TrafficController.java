package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.bean.*;
import com.atguigu.gmall.publisher.service.TrafficChannelStatsService;
import com.atguigu.gmall.publisher.service.TrafficKeywordsService;
import com.atguigu.gmall.publisher.util.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @ClassName TrafficController
 * @Package com.atguigu.gmall.publisher.controller
 * @Author CaiBW
 * @Create 24/02/25 下午 4:48
 * @Description
 */
@RestController
@RequestMapping("/gmall/realtime/traffic")
public class TrafficController {
    
    @Autowired
    TrafficKeywordsService trafficKeywordsService;
    @Autowired
    TrafficChannelStatsService trafficChannelStatsService;
    
    //各渠道
    @RequestMapping("/channelStats")
    public String getChannelStats(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtils.now();
        }
        List<TrafficUvCt> trafficUvCt = trafficChannelStatsService.getUvCt(date);
        List<TrafficSvCt> trafficSvCt = trafficChannelStatsService.getSvCt(date);
        List<TrafficPvPerSession> trafficPvPerSession = trafficChannelStatsService.getPvPerSession(date);
        List<TrafficDurPerSession> trafficDurPerSession = trafficChannelStatsService.getDurPerSession(date);
        List<TrafficUjRate> trafficUjRate = trafficChannelStatsService.getUjRate(date);
        //处理为空数据
        if (trafficUvCt == null) {
            return "";
        }
        if (trafficSvCt == null) {
            return "";
        }
        if (trafficPvPerSession == null) {
            return "";
        }
        if (trafficDurPerSession == null) {
            return "";
        }
        if (trafficUjRate == null) {
            return "";
        }
        
        String categories = trafficUvCt.stream().map(TrafficUvCt::getCh).collect(Collectors.joining("\",\""));
        String uvCtData = trafficUvCt.stream().map(TrafficUvCt::getUvCt).map(Objects::toString).collect(Collectors.joining(","));
        String svCtData = trafficSvCt.stream().map(TrafficSvCt::getSvCt).map(Objects::toString).collect(Collectors.joining(","));
        String pvPerSession = trafficPvPerSession.stream().map(TrafficPvPerSession::getPvPerSession).map(Objects::toString).collect(Collectors.joining(","));
        String durPerSession = trafficDurPerSession.stream().map(TrafficDurPerSession::getDurPerSession).map(Objects::toString).collect(Collectors.joining(","));
        String ujRate = trafficUjRate.stream().map(TrafficUjRate::getUjRate).map(Objects::toString).collect(Collectors.joining(","));
        return "{\"status\": 0,\n" +
            "  \"msg\": \"\",\n" +
            "  \"data\": {\n" +
            "    \"categories\": [\"" + categories + "\"],\n" +
            "    \"series\": [\n" +
            "      {\"name\": \"独立访客数\",\n" +
            "       \"data\": [" + uvCtData + "]},\n" +
            "      {\"name\": \"会话总数\",\n" +
            "       \"data\": [" + svCtData + "]},\n" +
            "      {\"name\": \"会话平均浏览页面数\",\n" +
            "       \"data\": [" + pvPerSession + "]},\n" +
            "      {\"name\": \"会话平均停留时长x100\",\n" +
            "       \"data\": [" + durPerSession + "]},\n" +
            "      {\"name\": \"跳出率\",\n" +
            "       \"data\": [" + ujRate + "]}\n" +
            "    ]\n" +
            "  }\n" +
            "}";
    }
    
    //关键字
    @RequestMapping("/keywords")
    public String getKeyWords(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtils.now();
        }
        List<TrafficKeywords> keywords = trafficKeywordsService.getKeywords(date);
        String data = keywords.stream().map(keyword -> "{\"name\":\"" + keyword.getKeyword() + "\",\"value\":" + keyword.getKeywordCount() + "}").collect(Collectors.joining(","));
        
        return "{\"status\": 0,\"msg\": \"\",\"data\": [" + data + "]" + "}";
    }
    
}
