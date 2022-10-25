package com.malf.dayfour;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据的重分区
 * keyBY     双重哈希
 * shuffle   随机
 * rebalance 轮询，需要网络
 * rescale   轮询，不需要网络，性能更好
 */
public class FlinkTransFormKeyByShuffleRebalanceRescale {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<String> ds = environment
                .socketTextStream("localhost", 7890)
                .map((MapFunction<String, String>) value -> value);

        ds.print().setParallelism(2);

        ds.keyBy((KeySelector<String, String>) value -> value).print("keyBy");
        DataStream<String> shuffle = ds.shuffle();
        shuffle.print("shuffle");
//        ds.rebalance().print("reBalance");
//        ds.rescale().print("rescale");

        environment.execute();

    }
}
