package com.malf.daysix;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口计算函数
 * 就是当窗口结束，要进行的计算逻辑，在此定义
 */
public class FlinkReduceWindowFunction {
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
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {

                        return Tuple2.of(value2.f0, value1.f1+value2.f1);
                    }
                })
                .print();

        environment.execute();
    }
}
