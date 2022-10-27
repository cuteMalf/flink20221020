package com.malf.daysix;

import com.malf.bean.WaterSensor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;


/**
 * 感受动态间隔，带来的窗口合并
 */
public class FlinkSessionWindowByProcessTimeWithDynamicGap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment
                .socketTextStream("localhost", 7890)
                .map((MapFunction<String, WaterSensor>) value -> {
                    String[] strings = value.split(",");
                    return new WaterSensor(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2]));
                })
                .keyBy(WaterSensor::getId)
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
                    @Override
                    public long extract(WaterSensor element) {
                        return element.getTs()*1000;
                    }
                }))
                .max("vc")
                .print();

        environment.execute();
    }
}
