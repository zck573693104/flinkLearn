package com.bigdata.udf;

import org.apache.flink.table.functions.AggregateFunction;

public class TestAggregateFunction extends AggregateFunction<Long, TestAggregateFunction.SumAll> {

    @Override
    public Long getValue(SumAll acc) {
        return acc.sum;
    }

    @Override
    public SumAll createAccumulator() {
        return new SumAll();
    }

    public void accumulate(SumAll acc, long iValue) {
        acc.sum += iValue;
    }


    public void retract(SumAll acc, long iValue) {
        acc.sum -= iValue;
    }

    public void merge(SumAll acc, Iterable<SumAll> it) {

        for (SumAll a : it) {
            acc.sum += a.sum;
        }
    }

    public void resetAccumulator(SumAll acc) {
        acc.sum = 0L;
    }

    public static class SumAll {
        public long sum = 0;
    }

}
