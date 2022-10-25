package com.malf.dayfour;

import com.malf.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTransFormReduce {
    /**
     * 聚合算子
     * 合并当前元素与上次聚合的结果，产生一个新的结果，
     * 是keyedStream的方法，在分组之后才能调用
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.socketTextStream("localhost",7890)
                .map((MapFunction<String, WaterSensor>) value -> {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
                    })
                .keyBy((KeySelector<WaterSensor, String>) value -> value.getId()).reduce((ReduceFunction<WaterSensor>) (value1, value2) -> new WaterSensor(value1.getId(),value1.getTs(),Math.max(value1.getVc(),value2.getVc())))
                .print();
        environment.execute();
    }


}
