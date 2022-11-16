package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static List<String> splitKeyWord(String keyWord) throws IOException {

        //创建集合用于存放结果数据
        ArrayList<String> resultList = new ArrayList<>();

        StringReader reader = new StringReader(keyWord);

        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        while (true) {
            Lexeme next = ikSegmenter.next();

            if (next != null) {
                String word = next.getLexemeText();
                resultList.add(word);
            } else {
                break;
            }
        }

        //返回结果数据
        return resultList;
    }

    public static void main(String[] args) throws IOException {

        System.out.println(splitKeyWord("Apple iPhoneXSMax (A2104) 256GB 深空灰色移动联通电信4G手机 双卡双待"));

    }
}
