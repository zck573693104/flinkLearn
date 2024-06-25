package com.bigdata.leetcode;

public class Tqueue<T> {
    private final T[] queue;
    private int first;
    private int end;
    private final int size;

    public Tqueue(int size) {
        this.size = size;
        queue = (T[]) new Object[size];
        first = 0; // 初始化为0，表示队列头部
        end = 0;   // 初始化为0，表示队列尾部（新元素将加入此处）
    }

    public void push(T value) {
        if (isFull()) {
            return; // 队列已满，无法添加新元素
        }
        queue[end] = value;
        end = (end + 1) % size; // 使用模运算循环队列
    }

    public T pop() {
        if (isEmpty()) {
            return null; // 队列为空，无法出队
        }
        T t = queue[first];
        queue[first] = null; // 帮助垃圾回收
        first = (first + 1) % size; // 移动first指针
        return t;
    }

    public T peek() {
        if (isEmpty()) {
            return null; // 队列为空，没有元素可以查看
        }
        return queue[first];
    }

    public void show(){
        for (int i = first; i != end; i = (i + 1) % size) { // 正确遍历循环队列
            System.out.print(queue[i] + " ");
        }
        System.out.println();
    }

    public boolean isEmpty() {
        return first == end;
    }

    public boolean isFull() {
        return (end + 1) % size == first;
    }

    public static void main(String[] args) {
        Tqueue<Integer> tqueue = new Tqueue<>(5);
        tqueue.push(1);
        tqueue.push(2);
        tqueue.push(3);
        System.out.println(tqueue.pop());
        System.out.println(tqueue.pop());
        tqueue.show();
        System.out.println(tqueue.peek());
        tqueue.show();
        System.out.println(tqueue.pop());
        System.out.println(tqueue.pop());
    }
}
