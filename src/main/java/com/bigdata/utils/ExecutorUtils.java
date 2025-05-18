package com.bigdata.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorUtils {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        executor.submit(() -> {
            System.out.println("task 1");
        });

        executor.submit(() -> {
            System.out.println("task 2");
        });
    }
}
