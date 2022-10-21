package com.malf.daytwo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 实现多并行度的自定义数据源
 */
public class FlinkUserDefineSourceTwo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        environment.addSource(new ParallelSourceFunction<String>() {
            private Boolean isRunning =true;
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning){
                    ctx.collect("testTwo");
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isRunning=false;
            }
        }).setParallelism(2).print();

        environment.execute();

    }
}
