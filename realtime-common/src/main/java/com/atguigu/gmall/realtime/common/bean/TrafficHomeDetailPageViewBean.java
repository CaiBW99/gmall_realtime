package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @ClassName TrafficHomeDetailPageViewBean
 * @Package com.atguigu.gmall.realtime.common.bean
 * @Author CaiBW
 * @Create 24/02/18 上午 11:49
 * @Description
 */
@Data
@AllArgsConstructor
@Builder
public class TrafficHomeDetailPageViewBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    String curDate;
    // 首页独立访客数
    Long homeUvCt;
    // 商品详情页独立访客数
    Long goodDetailUvCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
