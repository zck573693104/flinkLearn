package com.bigdata.leetcode;

public class LinkedListQueue<T> {
    private Node<T> head;
    private Node<T> tail;


    public void add(T value) {
        Node<T> newNode = new Node<>(value);
        if (head == null) {
            head = newNode;
            tail = newNode;
        } else {
           tail.next = newNode;
           tail = newNode;
        }
    }

    public static void main(String[] args) {
        LinkedListQueue<Integer> queue = new LinkedListQueue<>();
        queue.add(1);
        queue.add(2);
        queue.add(3);
    }

    private static class Node<T> {
        private T data;
        private Node<T> next;

        public Node(T data) {
            this.data = data;
            this.next = null;
        }
    }
}

