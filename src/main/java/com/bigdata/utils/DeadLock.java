package com.bigdata.utils;

public class DeadLock {
    private static final Object LOCK1 = new Object();
    private static final Object LOCK2 = new Object();

    public static void main(String[] args) {
        Thread thread1 = new Thread(() -> {
            synchronized (LOCK1) {
                System.out.println("获取锁1");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                synchronized (LOCK2) {
                    System.out.println("获取锁2");
                }
            }


        });
        Thread thread2 = new Thread(() -> {
            synchronized (LOCK2) {
                System.out.println("获取锁2");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                synchronized (LOCK1) {
                    System.out.println("获取锁1");
                }
            }


        });
        thread1.start();
        thread2.start();
    }
}
