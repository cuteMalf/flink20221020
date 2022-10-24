package com.malf.daythree;

import com.malf.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTransFormMap {
    /**
     * 转换算子，map
     * 一对一。一进一出
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        environment
                .socketTextStream("localhost",7890)
                .map((MapFunction<String, WaterSensor>) value -> {
                    String[] strings = value.split(" ");
                    WaterSensor waterSensor = new WaterSensor(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2]));
                    return waterSensor;
                })
                .print();

        environment.execute();
    }
}
