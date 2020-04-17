package com.flink.qnx.registerfunction;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * 行转列 内存不足需要研究bitmap
 * @param <T>
 */
public class LineToColumn<T> extends AggregateFunction<Set<T>, Set<T>> {

    @Override
    public Set<T> getValue(Set<T> resultSet) {
        return resultSet;
    }

    @Override
    public Set<T> createAccumulator() {
        return new HashSet<>();
    }

    public void accumulate(Set<T> accSet, T value) {

        accSet.add(value);
    }

    public void retract(Set<T> accSet, T value) {
        accSet.remove(value);
    }

    public void merge(Set<T> accSet, Iterable<Set<T>> it) {
        Iterator<Set<T>> iter = it.iterator();
        while (iter.hasNext()) {
            Set<T> set = iter.next();
            accSet.addAll(set);
        }
    }

    public void resetAccumulator(Set<T> accSet) {

        accSet.clear();
    }

}
