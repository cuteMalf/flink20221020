package com.malf.daythree;

import com.malf.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTransFormMax {
    /**
     * keyBy 分组后求最大值
     * 注意与maxBy的区别，主要在于非比较列
     * 非比较取第一次出现的值
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.socketTextStream("localhost",7890)
                .map((MapFunction<String, WaterSensor>) s -> {
                    String[] strings = s.split(" ");
                    return new WaterSensor(strings[0],Long.parseLong(strings[1]),Integer.parseInt(strings[2]));
                })
                .keyBy((KeySelector<WaterSensor, String>) waterSensor -> waterSensor.getId())
                .max("vc")
                .print();



        environment.execute();

    }
}
