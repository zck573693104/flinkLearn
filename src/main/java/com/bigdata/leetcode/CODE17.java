package com.bigdata.leetcode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CODE17 {
    Map<Character, String> phoneMap = new HashMap<>() {{
        put('2', "abc");
        put('3', "def");
        put('4', "ghi");
        put('5', "jkl");
        put('6', "mno");
        put('7', "pqrs");
        put('8', "tuv");
        put('9', "wxyz");
    }};
    List<String> res = new ArrayList<String>();
    StringBuffer buffer = new StringBuffer();
    public List<String> letterCombinations(String digits) {
        //解决快速寻找数组中所有存在目标字母的方法: Map加回溯
        if (digits.length() == 0)  return res;
        backtrack( digits, 0);
        return res;
    }
    public void backtrack(String digits, int index) {
        if (index == digits.length()) res.add(buffer.toString());
        else {
            char digit = digits.charAt(index);
            String letters = phoneMap.get(digit);
            for (int i = 0; i < letters.length(); i++) {
                buffer.append(letters.charAt(i));
                backtrack( digits, index+1);
                buffer.deleteCharAt(index);
            }
        }
    }
}
