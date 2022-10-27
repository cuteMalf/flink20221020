package com.malf.dayfive;

import com.malf.bean.UserBehavior;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * 计算 unique visitor
 * 统计流量的重要指标是网站的独立访客数
 */
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        environment
                .readTextFile("input/UserBehavior.csv")
                .map((MapFunction<String, UserBehavior>) value -> {
                    String[] strings = value.split(",");
                    return new UserBehavior(Long.parseLong(strings[0]),Long.parseLong(strings[1]),Integer.parseInt(strings[2]),strings[3],Long.parseLong(strings[4]));
                })
                .filter((FilterFunction<UserBehavior>) value -> value.getBehavior().equals("pv"))
                .process(new ProcessFunction<UserBehavior, String>() {
                    private HashSet<Long> hashSet = new HashSet<>();
                    @Override
                    public void processElement(UserBehavior value, ProcessFunction<UserBehavior, String>.Context ctx, Collector<String> out) {
                        hashSet.add(value.getUserId());
                        out.collect(hashSet.size()+" ");
                    }
                })
                .print();

        environment.execute();
    }
}
