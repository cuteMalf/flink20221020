package com.malf.daytwo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 简历kafka 数据源
 * 从kafka读取数据
 * 通用方式
 * cmd:kafka-console-producer.sh --broker-list hadoop162:9092 --topic sensor
 * flink version:1.12.x
 */
public class FlinkSourceKafkaOne {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        properties.setProperty("group.id","flinkSourceKafka");
        properties.setProperty("auto.offset.reset","latest");

        environment
                .addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties))
                .print("*");
        environment.execute();
    }
}
