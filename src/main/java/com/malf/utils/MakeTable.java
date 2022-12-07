package com.malf.utils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MakeTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        environment
                .readTextFile("input/new 1")
                .map((MapFunction<String, String>) value -> {
                    String[] strings = value.split("\t");
                    return strings[0]+" "+strings[1];
                })
                .print();
        environment.execute();
    }
}
