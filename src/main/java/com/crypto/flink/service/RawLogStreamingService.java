package com.crypto.flink.service;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka에서 수집한 원본 메시지를 간단히 로그로 출력하는 Flink 스트리밍 서비스
 */
@Service
public class RawLogStreamingService {

    private static final Logger logger = LoggerFactory.getLogger(RawLogStreamingService.class);

    @Autowired
    private StreamExecutionEnvironment env;

    @Autowired
    private KafkaSource<String> kafkaSource;

    @Value("${jobs.rawLogger.enabled:false}")
    private boolean jobEnabled;

    @PostConstruct
    public void initRawLoggerJob() {
        // 별도 스레드에서 Flink Job 시작
        CompletableFuture.runAsync(() -> startRawLoggerJob());
    }

    public void startRawLoggerJob() {
        try {
            if (!jobEnabled) {
                logger.info("⏸️ RawLogStreamingService 비활성화됨 (jobs.rawLogger.enabled=false)");
                return;
            }

            logger.info("🪵 원본 로그 스트리밍 Job을 시작합니다...");

            DataStream<String> inputStream = env.fromSource(
                    kafkaSource,
                    org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                    "Kafka Source - Raw Logger");

            // 간단 로깅: 수신한 원본 JSON 문자열을 그대로 로그에 출력
            inputStream
                    .map(value -> {
                        logger.info("📥 RAW: {}", value);
                        return value;
                    })
                    .name("Raw Logger");

            env.execute("Raw Logger Job");

        } catch (Exception e) {
            logger.error("❌ Raw Logger Job 실행 중 오류 발생: ", e);
        }
    }
}
