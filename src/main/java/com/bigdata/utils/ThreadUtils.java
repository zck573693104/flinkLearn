package com.bigdata.utils;

public class ThreadUtils {
    public static void main(String[] args) {
        Thread thread = new Thread(() -> {
            System.out.println("Thread is running...");
        });
        thread.start();
        thread.start();
    }
}
