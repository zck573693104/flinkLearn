package com.bigdata.leetcode;


public class CODE70 {
    public static void main(String[] args) {

        int n = 4;
        System.out.println(climbStairs(n));
    }
    public static int climbStairs(int nums) {
        if (nums <= 2) {return nums;}
        int p = 1;
        int q = 2;
        int res = 3;
        for (int i = 3; i < nums; i++) {
            p = q;
            q = res;
            res = p+q;
        }
        return res;
    }

    public static int climbStairs1(int nums) {
        if (nums <= 2) {return nums;}
        int[] dp = new int[nums + 1];
        dp[1] = 1;
        dp[2] = 2;
        for (int i = 3; i <= nums; i++) {
            dp[i] = dp[i - 1] + dp[i - 2];
        }
        return dp[nums];
    }


}
