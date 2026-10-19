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
import org.springframework.context.annotation.Scope;

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

    @Value("${kafka.events-topic:crypto-events}")
    private String eventsTopic;

    @Value("${kafka.group-id:crypto-flink-group}")
    private String groupId;

    @Value("${kafka.dead-letter-topic:crypto-trades-dlq}")
    private String deadLetterTopic;

    @Value("${kafka.metrics-topic:crypto-metrics}")
    private String metricsTopic;

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
     * 감지 이벤트(대량 체결, 급등·급락)를 보내는 싱크.
     */
    @Bean
    public KafkaSink<String> eventsSink() {
        return stringSink(eventsTopic);
    }

    /**
     * 파싱에 실패한 원본 메시지를 보내는 싱크.
     * 본류에서 조용히 버리면 수집 쪽 문제를 알 수 없으므로 별도 토픽에 남긴다.
     */
    @Bean
    public KafkaSink<String> deadLetterSink() {
        return stringSink(deadLetterTopic);
    }

    /**
     * 주기적으로 갱신되는 지표(체결강도 등)를 보내는 싱크.
     * 감지 이벤트와 토픽을 나눠야 소비하는 쪽이 종류별로 보존 기간과 처리 방식을 달리할 수 있다.
     */
    @Bean
    public KafkaSink<String> metricsSink() {
        return stringSink(metricsTopic);
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
     * StreamExecutionEnvironment 설정.
     * 환경에는 붙인 연산이 쌓이므로 Job 을 다시 띄울 때마다 새 인스턴스를 받도록 prototype 으로 둔다.
     */
    @Bean
    @Scope("prototype")
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
