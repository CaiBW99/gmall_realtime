package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.bean.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @ClassName TrafficVisitorStatsMapper
 * @Package com.atguigu.gmall.publisher.mapper
 * @Author CaiBW
 * @Create 24/02/25 下午 6:54
 * @Description 流量分时统计
 */
public interface TrafficChannelStatsMapper {
    // 1. 获取各渠道独立访客数
    @Select("select ch,\n" +
        "       sum(uv_ct) uv_ct\n" +
        "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
        "partition (par#{date})\n" +
        "group by ch;")
    List<TrafficUvCt> selectUvCt(@Param("date") Integer date);
    
    
    // 2. 获取各渠道会话数
    @Select("select ch,\n" +
        "       sum(sv_ct) sv_ct\n" +
        "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
        "         partition (par#{date})\n" +
        "group by ch;")
    List<TrafficSvCt> selectSvCt(@Param("date") Integer date);
    
    // 3. 获取各渠道会话平均页面浏览数
    @Select("select ch,\n" +
        "       sum(pv_ct) / sum(sv_ct) pv_per_session\n" +
        "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
        "partition (par#{date})\n" +
        "group by ch\n;")
    List<TrafficPvPerSession> selectPvPerSession(@Param("date") Integer date);
    
    // 4. 获取各渠道会话平均页面访问时长
    @Select("select ch,\n" +
        "       sum(dur_sum) / sum(sv_ct)/100 dur_per_session\n" +
        "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
        "         partition (par#{date})\n" +
        "group by ch;")
    List<TrafficDurPerSession> selectDurPerSession(@Param("date") Integer date);
    
    // 5. 获取各渠道跳出率
    @Select("select ch,\n" +
        "       sum(if(pv_ct=1,1,0)) / sum(sv_ct) uj_rate\n" +
        "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
        "         partition (par#{date})\n" +
        "group by ch;")
    List<TrafficUjRate> selectUjRate(@Param("date") Integer date);
}
