package com.bigdata.utils;

public class BitOperationExample {
    public static void main(String[] args) {
        int value = 0;

        // 设置第 0 位为 1
        value |= (1 << 0);
        System.out.println("设置第 0 位后: " + Integer.toBinaryString(value));

        // 设置第 5 位为 1
        value |= (1 << 5);
        System.out.println("设置第 5 位后: " + Integer.toBinaryString(value));

        // 清除第 0 位
        value &= ~(1 << 0);
        System.out.println("清除第 0 位后: " + Integer.toBinaryString(value));

        // 翻转第 5 位
        value ^= (1 << 5);
        System.out.println("翻转第 5 位后: " + Integer.toBinaryString(value));

        // 检查第 5 位是否为 1
        boolean isFifthBitSet = (value & (1 << 5)) != 0;
        System.out.println("第 5 位是否为 1: " + isFifthBitSet);
    }
}
