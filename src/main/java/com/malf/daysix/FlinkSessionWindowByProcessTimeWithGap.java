package com.malf.daysix;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 会话窗口
 * 会话窗口是有窗口的合并的
 * 会话窗口有分为两种，主要是对于会话间隔的区分，一是会话间隔是静态的，二是会话间隔是动态的
 * 重点会话窗口的合并
 */
public class FlinkSessionWindowByProcessTimeWithGap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment
                .socketTextStream("localhost",7890)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    String[] strings = value.split(" ");
                    for (String string : strings) {
                        out.collect(Tuple2.of(string,1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5L)))
                .sum(1)
                .print();

        environment.execute();


    }
}
