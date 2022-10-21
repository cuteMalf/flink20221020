package com.malf.dayone;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment
                .setParallelism(1)
                .socketTextStream("hadoop162",9999).flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
            for (String s : value.split(" ")) {
                out.collect(Tuple2.of(s,1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .sum(1)
                .print();

        streamExecutionEnvironment.execute();
    }
}
