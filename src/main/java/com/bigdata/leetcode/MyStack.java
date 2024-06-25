package com.bigdata.leetcode;

public class MyStack<T> {
    private final T[] statcks;
    private final int size;
    private int top;

    public MyStack(int size) {
        this.statcks = (T[]) new Object[size];
        this.top = -1;
        this.size = size;
    }

    public void push(T value) {
        if (top < size - 1) {
            statcks[++top] = value;
        }
    }

    public T pop() {
        if (top == -1) {
            return null;
        } else {
            T t = statcks[top--];
            statcks[top+1] = null;
            return t;
        }
    }

    public T peek() {
        if (top == -1) {
            return null;
        } else {
            return statcks[top--];
        }
    }

    public void show(){
        for (int i = 0; i < size; i++) {
            System.out.print(statcks[i]+" ");
        }
        System.out.println();
    }

    public static void main(String[] args) {
        MyStack<Integer> myStack = new MyStack<>(10);
        myStack.push(1);
        myStack.push(2);
        myStack.push(3);
        System.out.println(myStack.pop());
        myStack.show();
        System.out.println(myStack.peek());
        myStack.show();
        System.out.println(myStack.pop());
        myStack.show();
        System.out.println(myStack.pop());
        System.out.println(myStack.pop());
        System.out.println(myStack.pop());
        myStack.push(4);
        System.out.println(myStack.pop());
        System.out.println(myStack.pop());
    }
}
