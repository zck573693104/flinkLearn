package com.bigdata.leetcode;

public class MyLinkedListQueue<T> {
    private Node<T> head; // 队列头部（链表的头部）
    private Node<T> tail; // 队列尾部（链表的尾部）
    private int size; // 队列的大小

    // 入队操作
    public void enqueue(T item) {
        Node<T> newNode = new Node<>(item);
        if (tail == null) { // 如果链表为空，则新节点既是头节点也是尾节点
            head = tail = newNode;
        } else {
            // 否则，将新节点添加到链表末尾
            tail.next = newNode;
            tail = newNode; // 更新尾节点
        }
        size++;
    }

    // 出队操作
    public T dequeue() {
        if (isEmpty()) {
            throw new IllegalStateException("Queue is empty");
        }
        T item = head.data;
        head = head.next; // 更新头节点
        if (head == null) { // 如果链表变为空，则重置尾节点
            tail = null;
        }
        size--;
        return item;
    }

    // 查看队首元素
    public T peek() {
        if (isEmpty()) {
            throw new IllegalStateException("Queue is empty");
        }
        return head.data;
    }

    // 检查队列是否为空
    public boolean isEmpty() {
        return head == null;
    }

    // 获取队列的大小
    public int size() {
        return size;
    }

    // 主函数，用于测试
    public static void main(String[] args) {
        MyLinkedListQueue<Integer> queue = new MyLinkedListQueue<>();

        // 入队
        queue.enqueue(1);
        queue.enqueue(2);
        queue.enqueue(3);

        // 查看队首元素
        System.out.println("Peek: " + queue.peek()); // 输出：1

        // 出队
        System.out.println("Dequeue: " + queue.dequeue()); // 输出：1
        System.out.println("Dequeue: " + queue.dequeue()); // 输出：2
        System.out.println("Dequeue: " + queue.dequeue()); // 输出：3
        System.out.println("Dequeue: " + queue.dequeue()); // 输出：null


    }
    private class Node<T> {
        T data;
        Node<T> next;

        public Node(T data) {
            this.data = data;
        }
    }
}