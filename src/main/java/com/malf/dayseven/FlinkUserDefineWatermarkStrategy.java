package com.malf.dayseven;


import com.malf.bean.WaterSensor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 用户自定义 水位线策略
 * 1.周期性生成水位线
 * 2.间接性的生成数位先
 * 都需要集成 WatermarkGenerator 这个接口。
 */
public class FlinkUserDefineWatermarkStrategy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置整个环境的并行度，也就是全局并行度
        environment.setParallelism(1);
        //设置作业的运行模式，流处理，批处理或者是自动模式
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置周期性水位线，多长时间生成一次。（默认的是周期性生成水位线）
        environment.getConfig().setAutoWatermarkInterval(200);
        environment
                .socketTextStream("localhost",7890)
                .map((MapFunction<String, WaterSensor>) value -> {
            String[] strings = value.split(",");
            return new WaterSensor(strings[0],Long.parseLong(strings[1]),Integer.parseInt(strings[2]));
        })      //自定义水位线生成的策略
                .assignTimestampsAndWatermarks((WatermarkStrategy<WaterSensor>) context -> new userDefineWatermarkGenerator(3000L))
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .process(new ProcessWindowFunction<WaterSensor, Object, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<WaterSensor, Object, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<Object> out) {
                        String msg = "当前key: " + key
                                + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd()/1000 + ") 一共有 "
                                + elements.spliterator().estimateSize() + "条数据 ";
                        out.collect(msg);

                    }
                })
                .print();

        environment.execute();
    }

    public static class userDefineWatermarkGenerator implements WatermarkGenerator<WaterSensor>{

        //当前最大时间戳
        private long maxTimestamp;
        //允许的最大延迟时间
        private long outOfOrdernessMillis;

        public userDefineWatermarkGenerator(long outOfOrdernessMillis) {

            this.maxTimestamp = Long.MIN_VALUE+this.outOfOrdernessMillis+1;
            this.outOfOrdernessMillis = outOfOrdernessMillis;
        }

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            //间歇性
            Math.max(eventTimestamp,maxTimestamp);

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //周期性
            System.out.println("userDefineWatermarkGenerator.onPeriodicEmit"+"生成水位线");
            output.emitWatermark(new Watermark(maxTimestamp-outOfOrdernessMillis-1L));


        }
    }
}


