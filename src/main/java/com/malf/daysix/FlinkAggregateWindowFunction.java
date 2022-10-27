package com.malf.daysix;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口关闭时的计算函数
 * 与reduce函数不同的是，aggregate函数输入和输出，可以不同
 * aggregate 存在中间计算结果.
 * merge 会话窗口中的窗口叠加中，会用到，用于叠加计算
 */
public class FlinkAggregateWindowFunction {
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
                .aggregate(new AggregateFunction<Tuple2<String, Integer>,Tuple2<String, Integer> , Tuple2<String, Integer>>() {

                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        System.out.print("创建累加器");
                         return Tuple2.of(null,0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
                        System.out.println("调用累加器");
                        return Tuple2.of(value.f0, value.f1+accumulator.f1);
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                        System.out.println("返回累加器");
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        System.out.println("FlinkAggregateWindowFunction.merge");
                        return null;
                    }
                })
                .print();

        environment.execute();
    }
}
