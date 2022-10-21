package com.malf.daytwo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 用户自定义数据源
 * 此示例只能设置单个并行度
 */
public class FlinkUserDefineSourceOne {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
         
        environment
                .addSource(new SourceFunction<String>() {
                    private Boolean isRunning=true;

                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        while (isRunning){
                            ctx.collect("test");
                            Thread.sleep(1000);
                        }

                    }

                    @Override
                    public void cancel() {
                        isRunning=false;
                    }
                })
                .print();



        environment.execute();
    }

}
