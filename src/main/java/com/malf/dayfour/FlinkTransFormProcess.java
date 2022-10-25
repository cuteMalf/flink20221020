package com.malf.dayfour;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * 更为底层的，process函数，可以实现flink 中任何功能
 *
 */
public class FlinkTransFormProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment
                .socketTextStream("localhost",7890)
                .process(new ProcessFunction<String, String>() {
                        @Override
                        public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) {
                            String[] strings = value.split(" ");
                            for (String string : strings) {
                                out.collect(string);
                            }
                        }
                    })
                .keyBy((KeySelector<String, String>) value -> value)
                .process(new KeyedProcessFunction<String, String, String>() {
                    private HashMap<String, Integer> worldCount = new HashMap<>();
                    @Override
                    public void processElement(String value, KeyedProcessFunction<String, String, String>.Context ctx, Collector<String> out) {
                        if (worldCount.containsKey(value)){
                            Integer count = worldCount.get(value);
                            count++;
                            worldCount.put(value,count);

                        }
                        else {
                            worldCount.put(value,1);
                        }

                        out.collect(value+","+worldCount.get(value));
                    }
                })
                .print();

        environment.execute();
    }
}
