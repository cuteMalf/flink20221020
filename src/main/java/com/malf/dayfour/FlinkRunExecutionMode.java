package com.malf.dayfour;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *设置运行模式
 * batch or stream
 * 默认是stream
 * 优先级：命令行配置>代码配置>配置文件配置
 * 建议使用命令行配置
 */
public class FlinkRunExecutionMode {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);


        environment.execute();
    }
}
