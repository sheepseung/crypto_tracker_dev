package com.crypto.flink.consumer;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.context.AbsorptionDetector;
import com.crypto.flink.store.DetectionEventRepository;
import com.crypto.flink.store.IntensityBucketRepository;
import com.crypto.flink.store.MetricsStore;
import com.crypto.flink.stream.StreamBroadcaster;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Flink 가 쓴 결과 토픽을 읽어 조회용 저장소에 넣는다.
 * API 가 Flink 상태를 직접 읽지 않고 토픽을 거치므로 Job 과 API 를 따로 재시작할 수 있다.
 */
@Component
public class ResultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ResultConsumer.class);

    private final ObjectMapper objectMapper;
    private final MetricsStore metricsStore;
    private final DetectionEventRepository eventRepository;
    private final IntensityBucketRepository bucketRepository;
    private final AbsorptionDetector absorptionDetector;
    private final StreamBroadcaster broadcaster;

    public ResultConsumer(ObjectMapper objectMapper, MetricsStore metricsStore,
            DetectionEventRepository eventRepository, IntensityBucketRepository bucketRepository,
            AbsorptionDetector absorptionDetector, StreamBroadcaster broadcaster) {
        this.objectMapper = objectMapper;
        this.metricsStore = metricsStore;
        this.eventRepository = eventRepository;
        this.bucketRepository = bucketRepository;
        this.absorptionDetector = absorptionDetector;
        this.broadcaster = broadcaster;
    }

    // 지표는 최신 값만 의미가 있어 기동 시 쌓인 옛 메시지를 건너뛴다
    @KafkaListener(topics = "${kafka.metrics-topic}", groupId = "${kafka.api-group-id}",
            properties = "auto.offset.reset=latest")
    public void onMetric(String json) {
        try {
            TradeIntensity intensity = objectMapper.readValue(json, TradeIntensity.class);
            metricsStore.put(intensity);
            // 일별 요약용. 5분 경계 윈도우만 저장되고 나머지는 거른다
            if (bucketRepository.saveIfAligned(intensity)) {
                // 새 버킷이 생길 때만 흡수·공백을 본다. 같은 버킷을 다시 받아도 한 번만 판정한다
                for (DetectionEvent event : absorptionDetector.onBucket(IntensityBucketRepository.Bucket.of(intensity))) {
                    if (eventRepository.save(event)) {
                        logger.info("감지 이벤트: {}", event.getMessage());
                        broadcaster.publishEvent(event);
                    }
                }
            }
            broadcaster.publishMetric(intensity);
        } catch (JsonProcessingException e) {
            logger.warn("지표 메시지 파싱 실패: {}", json, e);
        }
    }

    // 이벤트는 이력이므로 처음부터 읽고, 중복은 저장소의 고유 제약으로 거른다
    @KafkaListener(topics = "${kafka.events-topic}", groupId = "${kafka.api-group-id}",
            properties = "auto.offset.reset=earliest")
    public void onEvent(String json) {
        try {
            DetectionEvent event = objectMapper.readValue(json, DetectionEvent.class);
            if (eventRepository.save(event)) {
                logger.info("감지 이벤트: {}", event.getMessage());
                broadcaster.publishEvent(event);
            }
        } catch (JsonProcessingException e) {
            logger.warn("이벤트 메시지 파싱 실패: {}", json, e);
        }
    }
}
