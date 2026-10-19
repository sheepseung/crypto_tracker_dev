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

    @Value("${kafka.dead-letter-topic:crypto-trades-dlq}")
    private String deadLetterTopic;

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
        return stringSink(outputTopic);
    }

    /**
     * 파싱에 실패한 원본 메시지를 보내는 싱크.
     * 본류에서 조용히 버리면 수집 쪽 문제를 알 수 없으므로 별도 토픽에 남긴다.
     */
    @Bean
    public KafkaSink<String> deadLetterSink() {
        return stringSink(deadLetterTopic);
    }

    private KafkaSink<String> stringSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
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
