package com.malf.daysix;

import com.malf.bean.UserBehavior;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 计算 page view
 * 用户每次打开一个页面便记录1次PV，多次打开同一页面则浏览量累计。
 */
public class PageView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setParallelism(4);
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        environment
                .readTextFile("input/UserBehavior.csv")
                .map((MapFunction<String, Tuple2<String,Long>>) value -> {
                    String[] strings = value.split(",");
                    return Tuple2.of(strings[0],1L);
                 })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .sum(1)
                .print();

        environment.execute();

    }
}
