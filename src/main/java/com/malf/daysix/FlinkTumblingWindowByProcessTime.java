package com.malf.daysix;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * flink 时间窗口
 * 基于处理时间的滚动窗口
 * 基于处理时间的窗口不会等待数据到来是才开始生成窗口，而是在程序时，窗口就一直在生成。
 * 数据只会落在，当时窗口打开时的那个窗口
 * 窗口函数一般在keyBY 只会使用，nonKeyBy的流后续使用窗口函数，并行度只有1.
 */
public class FlinkTumblingWindowByProcessTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
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
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5L)))
                .sum(1)
                .print();


        environment.execute();
    }
}
