package com.malf.daythree;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTransFormRichMap {
    /**
     * 使用RichMap富函数
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        environment
                .fromElements("1","2","3","4","5")
                .map(new RichMapFunction<String, String>() {

                        @Override
                        public void open(Configuration parameters) {
                            System.out.println("执行open方法");
                        }

                        @Override
                            public String map(String value) {
                                return value+"   *";
                            }

                         @Override
                         public void close() {
                             System.out.println("执行close方法");
                         }

                         @Override
                         public RuntimeContext getRuntimeContext() {
                             return super.getRuntimeContext();
                         }
                     }
                    )
                .print();





        environment.execute();
    }
}
