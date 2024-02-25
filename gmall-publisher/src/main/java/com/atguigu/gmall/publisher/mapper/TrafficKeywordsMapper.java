package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.bean.TrafficKeywords;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @ClassName TrafficKeywordsMapper
 * @Package com.atguigu.gmall.publisher.mapper
 * @Author CaiBW
 * @Create 24/02/25 下午 4:55
 * @Description
 */
@Mapper
public interface TrafficKeywordsMapper {
    @Select(
        "select\n" +
            "  keyword,\n" +
            "  sum(keyword_count) keywordCount\n" +
            "from dws_traffic_source_keyword_page_view_window\n" +
            "partition(par#{date})\n" +
            "group by keyword"
    )
    List<TrafficKeywords> selectKeywords(Integer date);
    
}
