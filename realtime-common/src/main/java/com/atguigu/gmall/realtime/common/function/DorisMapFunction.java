package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @ClassName DorisMapFunction
 * @Package com.atguigu.gmall.realtime.common.function
 * @Author CaiBW
 * @Create 24/02/18 上午 11:48
 * @Description
 */
public class DorisMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T value) throws Exception {
        SerializeConfig serializeConfig = new SerializeConfig();
        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSONObject.toJSONString(value, serializeConfig);
    }
}
