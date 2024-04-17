package com.bigdata.leetcode;

public class CODE27 {
    public static void main(String[] args) {
        int[] nums1 = {0};
        int m = 0;
        System.out.println(removeElement(nums1, m));
    }

    public static int removeElement(int[] nums, int val) {
        int i = 0;
        for (int j = 0; j < nums.length; j++) {
            if (nums[j] != val) {
                nums[i] = nums[j];
                i++;
            }
        }
        return i;
    }
}
