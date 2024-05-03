package com.bigdata.leetcode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class CODE121 {
    public static void main(String[] args) {
        int[] nums = {7, 1, 5, 3, 6, 4};
        System.out.println(maxProfit(nums));
    }

//    输入: nums = [1,2,3,4,5,6,7], k = 3
//    输出: [5,6,7,1,2,3,4]


    public static int maxProfit(int[] prices) {
        int min = Integer.MAX_VALUE;
        int maxDiff = 0;
        for (int price : prices) {
            if (min > price) {
                min = price;
            } else if (price - min > maxDiff) {
                maxDiff = price - min;
            }
        }
        return maxDiff;
    }

}