package com.bigdata.leetcode;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleThreadPool {

    // 工作线程类
    static class Worker extends Thread {
        private final BlockingQueue<Runnable> taskQueue;
        private final AtomicBoolean running = new AtomicBoolean(true);

        public Worker(BlockingQueue<Runnable> taskQueue) {
            this.taskQueue = taskQueue;
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    Runnable task = taskQueue.take();
                    task.run();
                    System.out.println("ok "+Thread.currentThread().getName());
                } catch (InterruptedException e) {
                    // 如果线程被中断（如shutdown时），则退出循环
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // 异常处理
                    e.printStackTrace();
                }
            }
        }

        void shutdown() {
            running.set(false);
            this.interrupt(); // 中断等待中的线程
        }
    }

    private final int threadCount;
    private final BlockingQueue<Runnable> taskQueue;
    private final Worker[] workers;

    public SimpleThreadPool(int threadCount) {
        this.threadCount = threadCount;
        this.taskQueue = new LinkedBlockingQueue<>();
        this.workers = new Worker[threadCount];

        for (int i = 0; i < threadCount; i++) {
            workers[i] = new Worker(taskQueue);
            workers[i].start();
        }
    }

    public void execute(Runnable task) {
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task interrupted", e);
        }
    }

    public void shutdown() {
        for (Worker worker : workers) {
            worker.shutdown();
        }
    }

    public static void main(String[] args) {
        SimpleThreadPool pool = new SimpleThreadPool(5);

        for (int i = 0; i < 20; i++) {
            pool.execute(new Runner(i));
        }

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        pool.shutdown();
    }

    private static class Runner implements Runnable {
        private final int taskId;

        public Runner(int taskId) {
            this.taskId = taskId;
        }

        @Override
        public void run() {
            System.out.println("Task " + taskId + " is running by " + Thread.currentThread().getName());
        }
    }
}
