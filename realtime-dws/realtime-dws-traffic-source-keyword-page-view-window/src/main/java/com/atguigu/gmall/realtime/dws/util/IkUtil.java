package com.atguigu.gmall.realtime.dws.util;

import org.jline.utils.Log;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * @ClassName IkUtil
 * @Package com.atguigu.gmall.realtime.dws.util
 * @Author CaiBW
 * @Create 24/02/18 上午 11:46
 * @Description
 */
public class IkUtil {
    public static Set<String> splitWord(String searchValue) {
        HashSet<String> result = new HashSet<>();
        StringReader reader = new StringReader(searchValue);
        IKSegmenter segmenter = new IKSegmenter(reader, true);
        try {
            Lexeme next = segmenter.next();
            while (next != null) {
                String word = next.getLexemeText();
                result.add(word);
                next = segmenter.next();
            }
        } catch (Exception e) {
            Log.error("分词失败 : " + searchValue);
        }
        return result;
    }
}
