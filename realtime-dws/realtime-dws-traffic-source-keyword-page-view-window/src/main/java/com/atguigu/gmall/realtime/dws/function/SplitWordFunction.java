package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.dws.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

/**
 * @ClassName SplitWordFunction
 * @Package com.atguigu.gmall.realtime.dws.function
 * @Author CaiBW
 * @Create 24/02/18 上午 11:46
 * @Description
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitWordFunction extends TableFunction<Row> {
    public void eval(String searchValue) {
        // 分词
        Set<String> words = IkUtil.splitWord(searchValue);
        for (String word : words) {
            collect(Row.of(word));
        }
    }
}
