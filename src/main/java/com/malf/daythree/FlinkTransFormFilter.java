package com.malf.daythree;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTransFormFilter {
    /**
     * 过滤算子
     * 将过滤条件为true 的数据发送到下游算子
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        environment
                .fromElements(1,2,3,4,5,6,7,8,9,0)
                .filter((FilterFunction<Integer>) value -> value%2==0)
                .print();
        environment.execute();
    }
}
