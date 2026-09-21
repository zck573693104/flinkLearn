package com.bigdata.lineage.graph;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 分层：Tarjan 缩点 + Kahn 最长路径。重点是环不能被漏掉，也不能把栈打穿。 */
class LayeredDagBuilderTest {

    /** 边写成 {@code a>b} 的形式，其余为孤立节点 */
    private static LayeredDagBuilder.Layers build(int nodeCount, String... edges) {
        List<String> nodes = new ArrayList<>();
        Map<String, Collection<String>> successors = new LinkedHashMap<>();
        for (String edge : edges) {
            String[] pair = edge.split(">");
            nodes.add(pair[0]);
            nodes.add(pair[1]);
            successors.computeIfAbsent(pair[0], key -> new ArrayList<>()).add(pair[1]);
        }
        for (int i = 0; i < nodeCount; i++) {
            nodes.add("iso" + i);
        }
        return LayeredDagBuilder.build(new LinkedHashSet<>(nodes), successors);
    }

    @Test
    void rankFollowsTheLongestChainNotTheFirstPath() {
        LayeredDagBuilder.Layers layers = build(0, "a>b", "b>c", "a>c");
        assertEquals(0, layers.rankOf("a"));
        assertEquals(1, layers.rankOf("b"), "b 有 a>c>b 之外的直链，不能被短路成 0 层");
        assertEquals(2, layers.rankOf("c"));
        assertFalse(layers.isCyclic("a"));
    }

    @Test
    void selfDependencyKeepsOneRankAndIsMarkedCyclic() {
        LayeredDagBuilder.Layers layers = build(0, "t>t");
        assertEquals(0, layers.rankOf("t"));
        assertTrue(layers.isCyclic("t"), "增量表自依赖必须标成环，UI 要画红边");
    }

    @Test
    void mutuallyReferencedTablesShareARank() {
        LayeredDagBuilder.Layers layers = build(0, "a>b", "b>a", "b>c");
        assertEquals(layers.rankOf("a"), layers.rankOf("b"), "强连通分量必须同层");
        assertTrue(layers.isCyclic("a"));
        assertTrue(layers.isCyclic("b"));
        assertEquals(layers.rankOf("b") + 1, layers.rankOf("c"));
    }

    @Test
    void isolatedNodesStayAtRankZero() {
        LayeredDagBuilder.Layers layers = build(3);
        assertEquals(0, layers.rankOf("iso0"));
        assertEquals(0, layers.rankOf("missing"));
        assertTrue(layers.getCyclic().isEmpty());
    }

    /** 递归 DFS 在语料规模会把栈打穿，这里用一条长链把迭代实现钉住 */
    @Test
    void longChainDoesNotOverflowTheStack() {
        int size = 20000;
        Map<String, Collection<String>> successors = new LinkedHashMap<>();
        List<String> nodes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String node = "t" + i;
            nodes.add(node);
            successors.put(node, Arrays.asList("t" + (i + 1)));
        }
        nodes.add("t" + size);
        LayeredDagBuilder.Layers layers = LayeredDagBuilder.build(nodes, successors);
        assertEquals(size, layers.rankOf("t" + size));
        assertEquals(0, layers.rankOf("t0"));
        assertTrue(layers.getCyclic().isEmpty());
    }

    @Test
    void rankCoversEveryNodeEvenWithoutEdges() {
        LayeredDagBuilder.Layers layers = build(0, "a>b");
        Set<String> ranked = new LinkedHashSet<>(layers.getRank().keySet());
        assertEquals(new LinkedHashSet<>(Arrays.asList("a", "b")), ranked);
    }
}
