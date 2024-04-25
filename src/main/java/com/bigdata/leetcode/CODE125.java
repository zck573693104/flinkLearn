package com.bigdata.leetcode;

public class CODE125 {
    public static void main(String[] args) {
//        A 65  Z 90
//         a 97  z 122
        String nums1 = "0p";
        System.out.println(isPalindrome1(nums1));
    }

    public static boolean isPalindrome1(String num) {
        num = num.toLowerCase();
        StringBuilder sb = new StringBuilder();
       for (int i = 0; i < num.length() ; i++) {
           if (Character.isLetterOrDigit(num.charAt(i))) {
               sb.append(num.charAt(i));
           }
       }
        StringBuilder newS = new StringBuilder(sb);
       return newS.compareTo(sb.reverse()) == 0;
    }


    public static boolean isPalindrome(String num) {
        int left = 0;
        int right = String.valueOf(num).length() - 1;
        num = num.toLowerCase();
        char[] nums = num.toCharArray();
        while (left < right) {
            while (left != right) {
                if (Character.isLetterOrDigit(nums[left])) {
                    break;
                } else {
                    left++;
                }
            }
            while (left != right) {
                if (Character.isLetterOrDigit(nums[right])) {
                    break;
                } else {
                    right--;
                }
            }
            if (nums[left] != nums[right]) {
                return false;
            }
            left++;
            right--;
        }
        return true;
    }
}
