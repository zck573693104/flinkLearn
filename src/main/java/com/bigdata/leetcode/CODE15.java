package com.bigdata.leetcode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CODE15 {
    public static void main(String[] args) {
        int[] nums1 = {-1,0,1,2,-1,-4};
        System.out.println(threeSum(nums1));
    }

    public static List<List<Integer>> threeSum(int[] num) {
        List<List<Integer>> res = new ArrayList<>();

        Arrays.sort(num);
        for (int i = 0; i < num.length; i++) {
            if (num[i] > 0) {
                break;
            }
            int l = i + 1;
            int r = num.length - 1;
            if (i>0 && num[i]  == num[i-1]) {
                continue;
            }
            while (l < r) {
                int sum = num[i] + num[l] + num[r];
                if (sum == 0) {
                    res.add(Arrays.asList(num[i], num[l], num[r]));

                    while (l < r && num[l] == num[l + 1]) {
                        l++;
                    }
                    while (l < r && num[r] == num[r - 1]) {
                        r--;
                    }
                    l++;
                    r--;
                } else if (sum < 0) {
                    l++;
                } else if (sum > 0) {
                    r--;
                }
            }
        }
        return res;
    }
}
