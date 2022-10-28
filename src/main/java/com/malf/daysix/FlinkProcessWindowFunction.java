package com.malf.daysix;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 用于窗口结束们开始计算
 * processWindowFunction 是全量计算，因此性能会较差。但是这个接口可以拿到运行时上下文
 * 与其它windowfunction相比，它会等到这个窗口的所有数据都到来才会触发一次计算。
 * 
 */
public class FlinkProcessWindowFunction {
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
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) {
                        // key 上下文 这个窗口内的元素 收集器
                        System.out.println(context.window().getStart());

                        long sum=0;
                        for (Tuple2<String, Integer> element : elements) {
                            sum+=element.f1;
                        }
                        out.collect(s+""+sum);

                        System.out.println(context.window().getEnd());

                    }
                })
                .print();

        environment.execute();
    }
}
