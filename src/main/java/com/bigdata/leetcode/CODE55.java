package com.bigdata.leetcode;

class CODE55 {
    public static void main(String[] args) {
        int[] nums = {4, 3, 1, 1, 4};
        System.out.println(canJump(nums));
    }

    public static boolean canJump(int[] prices) {
        int n = prices.length;
        int rightmost = 0;
        for (int i = 0; i < n; i++) {
            if (i <= rightmost) {
                // 因为每走出一步，计算当前的最远的距离
                rightmost = Math.max(rightmost, i + prices[i]);
                if (rightmost >= n - 1) {
                    return true;
                }
            }
        }
        return false;
    }
}