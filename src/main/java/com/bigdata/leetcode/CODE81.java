package com.bigdata.leetcode;

import java.util.Arrays;

public class CODE81 {
    public static void main(String[] args) {
        int[] nums1 = {0};
        int[] nums2 = {1};
        int m = 0;
        int n = 1;
        merge(nums1, m, nums2, n);
        System.out.println(Arrays.toString(nums1));
    }

    public static void merge(int[] nums1, int m, int[] nums2, int n) {
        int endSize = nums1.length;
        while (n > 0) {
            if (m > 0 && nums1[m - 1] > nums2[n - 1]) {
                nums1[endSize - 1] = nums1[m - 1];
                m--;
            } else {
                nums1[endSize - 1] = nums2[n - 1];
                n--;
            }
            endSize--;
        }
    }
}
