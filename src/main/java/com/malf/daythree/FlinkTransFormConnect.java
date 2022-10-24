package com.malf.daythree;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class FlinkTransFormConnect {
    /**
     * 合并流，只能机械的合并一条流，两条流的类型可以不一样
     *
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Integer> integerDataStreamSource = environment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
        DataStreamSource<Character> characterDataStreamSource = environment.fromElements('a', 'b', 'c', 'd', 'e', 'f');

        ConnectedStreams<Integer, Character> connectedStreams = integerDataStreamSource.connect(characterDataStreamSource);
        SingleOutputStreamOperator<String> streamOperator = connectedStreams.map(new CoMapFunction<Integer, Character, String>() {
            @Override
            public String map1(Integer value) {
                return value + "-";
            }

            @Override
            public String map2(Character value) {
                return value + "*";
            }
        });

        streamOperator.print();

        environment.execute();

    }
}
