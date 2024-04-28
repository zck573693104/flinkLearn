package com.bigdata.leetcode;

import java.util.Arrays;

public class CODE169 {
    public static void main(String[] args) {

        int[] mums = {3, 2, 2, 2, 3};
        System.out.println(majorityElement(mums));
    }

    public static int majorityElement(int[] nums) {
        if (nums.length <= 2) {
            return nums[0];
        }
        Arrays.sort(nums);
        int n = nums.length / 2;
        int l = nums[0];
        int r = nums[n];
        int rRight = nums[n + 1];
        return (l == rRight) ? l : r;
    }


}
