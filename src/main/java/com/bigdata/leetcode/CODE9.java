package com.bigdata.leetcode;

import java.util.Arrays;

public class CODE9 {
    public static void main(String[] args) {
        int nums1 = 22;
        System.out.println(isPalindrome(nums1));
    }

    public static boolean isPalindrome(int num) {
        if (num<0) {
            return false;
        }
        int left = 0;
        int right = String.valueOf(num).length()-1;
        char [] nums = String.valueOf(num).toCharArray();
        while (left<right) {
            if (nums[left]!=nums[right]) {
                return false;
            }
            left++;
            right--;
        }
        return true;
    }
}
