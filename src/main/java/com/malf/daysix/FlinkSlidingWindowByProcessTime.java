package com.malf.daysix;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * flink 滑动窗口
 * sliding window
 */
public class FlinkSlidingWindowByProcessTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        environment
                .socketTextStream("localhost",7890)
                .keyBy((KeySelector<String, String>) value -> value)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(6),Time.seconds(3)))
                .max(0)
                .print();






        environment.execute();
    }
}
