package com.crypto.flink.config;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Flink Job 설정을 위한 Configuration 클래스
 * Kafka 소스와 싱크, 체크포인트 설정 등을 관리
 */
@Configuration
public class FlinkJobConfig {

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.input-topic:crypto-trades}")
    private String inputTopic;

    @Value("${kafka.output-topic:crypto-analysis}")
    private String outputTopic;

    @Value("${kafka.group-id:crypto-flink-group}")
    private String groupId;

    /**
     * Kafka 소스 설정
     */
    @Bean
    public KafkaSource<String> kafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /**
     * Kafka 싱크 설정
     */
    @Bean
    public KafkaSink<String> kafkaSink() {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }

    /**
     * StreamExecutionEnvironment 설정
     */
    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 체크포인트 설정
        env.enableCheckpointing(10000); // 10초마다 체크포인트
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000); // 1분 타임아웃
        checkpointConfig.setMinPauseBetweenCheckpoints(5000); // 최소 5초 간격
        checkpointConfig.setMaxConcurrentCheckpoints(1); // 동시 체크포인트 1개

        // 병렬성 설정
        env.setParallelism(1);

        return env;
    }
}
