package com.bigdata.leetcode;

import java.util.Arrays;

public class CODE26 {
    public static void main(String[] args) {
        int[] nums1 = {0,0,1,1,1,2,2,3,3,4};
        System.out.println(removeDuplicates(nums1));
        System.out.println(Arrays.toString(nums1));
    }

    public static int removeDuplicates(int[] nums) {
        if(nums == null || nums.length == 0) return 0;
        int p = 0;
        int q = 1;
        while(q < nums.length){
            if(nums[p] != nums[q]){
                nums[p + 1] = nums[q];
                p++;
            }
            q++;
        }
        return p + 1;
    }
}
