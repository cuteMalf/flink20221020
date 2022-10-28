package com.malf.dayseven;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 用户自定义 水位线策略
 * 1.周期性生成水位线
 * 2.间接性的生成数位先
 * 都需要集成 WatermarkGenerator 这个接口。
 */
public class FlinkUserDefineWatermarkStrategy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);





        environment.execute();
    }
}
