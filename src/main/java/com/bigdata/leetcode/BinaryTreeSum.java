package com.bigdata.leetcode;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class BinaryTreeSum {
    private static class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;

        TreeNode(int val) {
            this.val = val;
        }
    }

    /**
     * 计算二叉树所有节点值之和。
     * @param root 二叉树的根节点。
     * @return 所有节点值之和。
     */
    public static int sumOfTree(TreeNode root) {
        if (root == null) {
            return 0; // 空树的和为0
        }
        List<Integer> list = new ArrayList<>();
        int total = 0;
        Stack stack = new Stack<>();
        stack.push(root.val);
        while (root.left != null) {
            stack.push(root.left.val);
        }
        stack.peek();

        return total;
    }

    public static void main(String[] args) {
        // 构建一个简单的二叉树作为示例
        //       1
        //      / \
        //     2   3
        //    / \   \
        //   4   5   6
        // 124+125+136=363
        TreeNode tree = new TreeNode(1);
        tree.left = new TreeNode(2);
        tree.right = new TreeNode(3);
        tree.left.left = new TreeNode(4);
        tree.left.right = new TreeNode(5);
        tree.right.right = new TreeNode(6);
        System.out.println(124+125+136);
        // 计算并打印二叉树的节点值之和
        System.out.println("Sum of all nodes in the binary tree is: " + sumOfTree(tree));
    }
}
