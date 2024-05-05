package com.bigdata.leetcode;

class CODE122 {
    public static void main(String[] args) {
        int[] nums = {7, 1, 5, 3, 6, 4};
        System.out.println(maxProfit(nums));
    }

//    输入: nums = [1,2,3,4,5,6,7], k = 3
//    输出: [5,6,7,1,2,3,4]

    // 每天都在涨，那就每天都卖等同于n-i的利润
    public static int maxProfit(int[] prices) {
       int profit = 0;
       for (int i = 1; i < prices.length; i++) {
           int temp = prices[i] - prices[i - 1];
           if (temp > 0) {
               profit += temp;
           }
       }
       return profit;
    }
}