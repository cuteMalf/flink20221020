package com.malf.dayone;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = environment.readTextFile("input/word.txt");
        AggregateOperator<Tuple2<String, Long>> sum = dataSource.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
            String[] s = value.split(" ");
            for (String s1 : s) {
                out.collect(Tuple2.of(s1, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG)).groupBy(0).sum(1);
        System.out.println(sum);
        sum.print();
        //运行结束关闭环境
    }
}
