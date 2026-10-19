package com.crypto.flink.consumer;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.store.DetectionEventRepository;
import com.crypto.flink.store.MetricsStore;
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

    public ResultConsumer(ObjectMapper objectMapper, MetricsStore metricsStore,
            DetectionEventRepository eventRepository) {
        this.objectMapper = objectMapper;
        this.metricsStore = metricsStore;
        this.eventRepository = eventRepository;
    }

    // 지표는 최신 값만 의미가 있어 기동 시 쌓인 옛 메시지를 건너뛴다
    @KafkaListener(topics = "${kafka.metrics-topic}", groupId = "${kafka.api-group-id}",
            properties = "auto.offset.reset=latest")
    public void onMetric(String json) {
        try {
            metricsStore.put(objectMapper.readValue(json, TradeIntensity.class));
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
            }
        } catch (JsonProcessingException e) {
            logger.warn("이벤트 메시지 파싱 실패: {}", json, e);
        }
    }
}
