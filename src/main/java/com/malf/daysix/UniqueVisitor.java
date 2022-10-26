package com.malf.daysix;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 计算 unique visitor
 * 统计流量的重要指标是网站的独立访客数
 */
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        environment
                .readTextFile("input/UserBehavior.csv")
                .map((MapFunction<String, Tuple2<String,Long>>) value -> {
                    String[] strings = value.split(",");
                    return Tuple2.of(strings[0],1L);
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .sum(1)
                .map((MapFunction<Tuple2<String, Long>, String>) value -> value.f0)
                .process(new ProcessFunction<String, Long>() {
                    private Long count=0L;
                    @Override
                    public void processElement(String value, ProcessFunction<String, Long>.Context ctx, Collector<Long> out) throws Exception {
                        count++;
                        out.collect(count);
                    }
                })
                .print();

        environment.execute();
    }
}
