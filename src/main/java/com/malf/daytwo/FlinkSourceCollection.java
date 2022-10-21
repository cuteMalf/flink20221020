package com.malf.daytwo;

import com.malf.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class FlinkSourceCollection {
    /**
     * 从集合获取数据源
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        environment.fromCollection(Arrays.asList( new WaterSensor("ws_001", 1577844001L, 45),
                                                new WaterSensor("ws_002", 1577844015L, 43),
                                                new WaterSensor("ws_003", 1577844020L, 42)))
                .print();
        environment.execute();

        //输入元素，当作数据源
        environment.fromElements(1,2,3).print();
        environment.execute();
    }
}
