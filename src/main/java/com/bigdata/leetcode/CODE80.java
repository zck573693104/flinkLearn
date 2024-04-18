package com.bigdata.leetcode;

import java.util.Arrays;

public class CODE80 {
    public static void main(String[] args) {
        int[] nums1 = {0, 0, 0, 1, 1, 1};
        System.out.println(removeDuplicates(nums1));
        System.out.println(Arrays.toString(nums1));
    }

    public static int removeDuplicates(int[] nums) {
        return process2(nums, 2);
    }

    static int process2(int[] nums, int k) {
        int n = nums.length;
        if (n <= 2) {
            return n;
        }
        int slow = 2, fast = 2;
        while (fast < n) {
            if (nums[slow - 2] != nums[fast]) {
                nums[slow] = nums[fast];
                ++slow;
            }
            ++fast;
        }
        return slow;
    }

    static int process(int[] nums, int k) {
        int u = 0;
        for (int x : nums) {
            if (u < k || nums[u - k] != x) {
                nums[u++] = x;
            }
        }
        return u;
    }
}
