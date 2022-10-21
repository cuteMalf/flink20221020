package com.malf.daytwo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * kafka数据源的第二种实现方式，来自官方文档的实现，基于版本flink1.13.6
 * address：https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/
 * cmd:kafka-console-producer.sh --broker-list hadoop162:9092 --topic sensor
 * flink 1.12.x 之前只能用通用的方式
 */
public class FlinkSourceKafkaTwo {
    public static void main(String[] args) throws Exception {
        String brokers = "hadoop162:9092,hadoop163:9092,hadoop164:9092";
        KafkaSource<String> sensor = KafkaSource
                .<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("sensor")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        environment
                .fromSource(sensor, WatermarkStrategy.noWatermarks(),"kafkaSource")
                .print();
        environment.execute();

    }
}
