package com.bigdata.leetcode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class CODE189 {
    public static void main(String[] args) {
        int[] nums = {1,2};
        int k = 3;
        rotate(nums, k);
        System.out.println(Arrays.toString(nums));
    }

//    输入: nums = [1,2,3,4,5,6,7], k = 3
//    输出: [5,6,7,1,2,3,4]


    public static void rotate(int[] nums, int k) {
        if (k > nums.length) {
            k = k % nums.length;
        }
        List<Integer> lists = new ArrayList<>(nums.length);
        for (int num : nums) {
            lists.add(num);
        }
        List<Integer> end = lists.subList(nums.length - k, nums.length);
        end.addAll(lists.subList(0, nums.length - k));
        for (int i = 0; i < end.size(); i++) {
            nums[i] = end.get(i);
        }
    }


    public static void rotate1(int[] nums, int k) {
        int n = nums.length;
        if (k < nums.length) {
            int i = 0;
            while (k > 0) {
                int temp = nums[n - k];
                int f = nums[i];
                nums[i] = temp;
                nums[n - k] = f;
                System.out.println(Arrays.toString(nums));
                i++;
                k--;

            }
        }
    }
}