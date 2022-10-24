package com.malf.daythree;

import com.malf.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用maxBy 计算根据keyBy分组后的最大值
 * 注意与max 对比 ，非比较列的值的选取规则
 * 在最大值相同的时候,非比较列取第一个(true)
 * 在最大值相同的时候,非比较列取最新的一个(false)
 */
public class FlinkTransFormMaxBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.socketTextStream("localhost",7890)
                .map((MapFunction<String, WaterSensor>) s -> {
                    String[] strings = s.split(" ");
                    return new WaterSensor(strings[0],Long.parseLong(strings[1]),Integer.parseInt(strings[2]));
                })
                .keyBy((KeySelector<WaterSensor, String>) waterSensor -> waterSensor.getId())
                //.maxBy("vc",true)
                .maxBy("vc",false)
                .print();



        environment.execute();

    }
}
