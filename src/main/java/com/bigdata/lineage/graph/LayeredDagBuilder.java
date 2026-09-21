package com.bigdata.lineage.graph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 分层：Tarjan 缩强连通 → 在缩点 DAG 上按拓扑序跑最长路径得 rank。
 *
 * <p>必须先缩点：增量表自依赖（{@code INSERT INTO dwd.t SELECT ... FROM dwd.t}）与互相引用的
 * 两张表都是真实语料里存在的环，直接跑 Kahn 会把环上的节点整批漏掉，分层就缺一层。
 * 环上的节点同层，调用方按 {@link Layers#isCyclic(String)} 把回边标红。
 */
public final class LayeredDagBuilder {

    private LayeredDagBuilder() {
    }

    /** 分层结果：节点 → 层号，以及落在环上的节点 */
    public static final class Layers {

        private final Map<String, Integer> rank;
        private final Set<String> cyclic;

        Layers(Map<String, Integer> rank, Set<String> cyclic) {
            this.rank = Collections.unmodifiableMap(rank);
            this.cyclic = Collections.unmodifiableSet(cyclic);
        }

        public int rankOf(String node) {
            Integer hit = rank.get(node);
            return hit == null ? 0 : hit;
        }

        public Map<String, Integer> getRank() {
            return rank;
        }

        public Set<String> getCyclic() {
            return cyclic;
        }

        public boolean isCyclic(String node) {
            return cyclic.contains(node);
        }
    }

    /**
     * @param nodes      全部节点
     * @param successors 出边（数据流方向：来源 → 目标）；缺失的键按无出边处理
     */
    public static Layers build(Collection<String> nodes,
                               Map<String, ? extends Collection<String>> successors) {
        Map<String, Integer> component = new LinkedHashMap<>();
        List<List<String>> groups = new ArrayList<>();
        tarjan(nodes, successors, component, groups);

        int count = groups.size();
        Map<Integer, Integer> inDegree = new LinkedHashMap<>();
        Map<Integer, Set<Integer>> compSuccessors = new LinkedHashMap<>();
        Set<Integer> selfLooped = new HashSet<>();
        for (int c = 0; c < count; c++) {
            inDegree.put(c, 0);
        }
        for (String node : nodes) {
            Integer from = component.get(node);
            Collection<String> outs = successors.get(node);
            if (from == null || outs == null) {
                continue;
            }
            for (String to : outs) {
                Integer target = component.get(to);
                if (target == null) {
                    continue;
                }
                if (from.equals(target)) {
                    selfLooped.add(from);
                } else if (successors(from, target, compSuccessors)) {
                    inDegree.merge(target, 1, Integer::sum);
                }
            }
        }

        Map<Integer, Integer> compRank = new LinkedHashMap<>();
        Deque<Integer> queue = new ArrayDeque<>();
        for (Map.Entry<Integer, Integer> entry : inDegree.entrySet()) {
            compRank.put(entry.getKey(), 0);
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }
        // 拓扑序上的松弛：rank 取到本节点的最长链长度，源头层号为 0
        int settled = 0;
        while (!queue.isEmpty()) {
            Integer current = queue.poll();
            settled++;
            for (Integer next : compSuccessors.getOrDefault(current, Collections.<Integer>emptySet())) {
                compRank.merge(next, compRank.get(current) + 1, Math::max);
                if (inDegree.merge(next, -1, Integer::sum) == 0) {
                    queue.add(next);
                }
            }
        }
        if (settled < count) {
            throw new IllegalStateException("缩点之后仍有环，Tarjan 结果不正确");
        }

        Map<String, Integer> rank = new LinkedHashMap<>();
        Set<String> cyclic = new LinkedHashSet<>();
        for (String node : nodes) {
            Integer c = component.get(node);
            if (c == null) {
                continue;
            }
            rank.put(node, compRank.get(c));
            if (groups.get(c).size() > 1 || selfLooped.contains(c)) {
                cyclic.add(node);
            }
        }
        return new Layers(rank, cyclic);
    }

    private static boolean successors(Integer from, Integer to,
                                      Map<Integer, Set<Integer>> compSuccessors) {
        Set<Integer> outs = compSuccessors.get(from);
        if (outs == null) {
            outs = new LinkedHashSet<>();
            compSuccessors.put(from, outs);
        }
        return outs.add(to);
    }

    /**
     * 迭代版 Tarjan：语料里的字段图上千节点，递归 DFS 会把栈打穿。
     */
    private static void tarjan(Collection<String> nodes,
                               Map<String, ? extends Collection<String>> successors,
                               Map<String, Integer> component, List<List<String>> groups) {
        Map<String, Integer> index = new HashMap<>();
        Map<String, Integer> low = new HashMap<>();
        Deque<String> stack = new ArrayDeque<>();
        Set<String> onStack = new HashSet<>();
        int[] counter = new int[1];

        for (String root : nodes) {
            if (index.containsKey(root)) {
                continue;
            }
            Deque<Frame> calls = new ArrayDeque<>();
            calls.push(new Frame(root, successors.get(root)));
            while (!calls.isEmpty()) {
                Frame frame = calls.peek();
                String node = frame.node;
                if (frame.entering) {
                    frame.entering = false;
                    index.put(node, counter[0]);
                    low.put(node, counter[0]);
                    counter[0]++;
                    stack.push(node);
                    onStack.add(node);
                }
                boolean descended = false;
                while (frame.neighbors.hasNext()) {
                    String next = frame.neighbors.next();
                    if (!index.containsKey(next)) {
                        calls.push(new Frame(next, successors.get(next)));
                        descended = true;
                        break;
                    }
                    if (onStack.contains(next)) {
                        low.put(node, Math.min(low.get(node), index.get(next)));
                    }
                }
                if (descended) {
                    continue;
                }
                calls.pop();
                if (low.get(node).equals(index.get(node))) {
                    List<String> group = new ArrayList<>();
                    String member;
                    do {
                        member = stack.pop();
                        onStack.remove(member);
                        group.add(member);
                    } while (!member.equals(node));
                    int id = groups.size();
                    groups.add(group);
                    for (String done : group) {
                        component.put(done, id);
                    }
                }
                if (!calls.isEmpty()) {
                    String parent = calls.peek().node;
                    low.put(parent, Math.min(low.get(parent), low.get(node)));
                }
            }
        }
    }

    private static final class Frame {

        private final String node;
        private final Iterator<String> neighbors;
        private boolean entering = true;

        private Frame(String node, Collection<String> neighbors) {
            this.node = node;
            this.neighbors = neighbors == null
                    ? Collections.<String>emptyList().iterator() : neighbors.iterator();
        }
    }
}
