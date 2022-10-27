package com.malf.dayfive;

import com.malf.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



/**
 * 广告点击日志
 * 统计广告点击
 */
public class FlinkAdvertisementClick {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
//        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        environment
                .readTextFile("input/AdClickLog.csv")
                .map((MapFunction<String, AdsClickLog>) value -> {
                    String[] strings = value.split(",");
                    return new AdsClickLog(Long.parseLong(strings[0]),Long.parseLong(strings[1]),strings[2],strings[3],Long.parseLong(strings[4]));
                })
                .map((MapFunction<AdsClickLog, Tuple2<Tuple2<String, Long>, Long>>) value -> Tuple2.of(Tuple2.of(value.getProvince(), value.getAdId()),1L))
                .returns(Types.TUPLE(Types.TUPLE(Types.STRING,Types.LONG),Types.LONG))
                .keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) {
                        return value.f0;
                    }
                })
                .sum(1)
                .print();

        environment.execute();
    }

}
