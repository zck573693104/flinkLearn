package com.bigdata.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ExecutorUtils {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        singleThread();
         fixThread();
    }
    private static void fixThread() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        Future<?> f1 =  executor.submit(() -> {
            System.out.println("task 1");
        });
        f1.get();
        Future<?> f2 =   executor.submit(() -> {
            System.out.println("task 2");
        });
        f2.get();
    }
    private static void singleThread() {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        executor.submit(() -> {
            System.out.println("task 1");
        });

        executor.submit(() -> {
            System.out.println("task 2");
        });
    }
}
