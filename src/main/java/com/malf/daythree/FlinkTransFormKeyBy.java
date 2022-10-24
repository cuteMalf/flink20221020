package com.malf.daythree;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTransFormKeyBy {
    /**
     * keyBy 按照key进行分组或者是分流
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);

        SingleOutputStreamOperator<String> mapStream = environment
                .socketTextStream("localhost", 7890)
                .map((MapFunction<String, String>) value -> value)
                .setParallelism(2);

        KeyedStream<String, String> keyedStream = mapStream.keyBy((KeySelector<String, String>) value -> value);


        mapStream.print().setParallelism(2);
        keyedStream.print("分组数据：");

        environment.execute();
    }
}
