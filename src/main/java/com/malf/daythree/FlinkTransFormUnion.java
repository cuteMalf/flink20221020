package com.malf.daythree;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTransFormUnion {
    /**
     * 合并流 union
     * 可以是多个流链接
     * 多个流的类型必须相同
     * 类似SQL中的union
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Integer> integerDataStreamSource = environment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
        DataStreamSource<Integer> integerDataStreamSource1 = environment.fromElements(11, 12, 13);

        DataStream<Integer> unionStream = integerDataStreamSource.union(integerDataStreamSource1);

        unionStream.print();

        environment.execute();
    }
}
