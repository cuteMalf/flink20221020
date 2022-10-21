package com.malf.daytwo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkSourceDirectory {
    /**
     * 从文件夹中或者文件中读取数据
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        environment
                .readTextFile("input")
                .print();
        environment.execute();
    }
}
