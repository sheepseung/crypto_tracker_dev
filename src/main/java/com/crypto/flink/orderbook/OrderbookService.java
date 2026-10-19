package com.crypto.flink.orderbook;

import com.crypto.flink.store.DetectionEventRepository;
import com.crypto.flink.stream.StreamBroadcaster;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * F9 호가 불균형. 호가 토픽을 최신 위치부터 읽어 종목별 쏠림 스냅샷을 유지하고, 오래 이어지는 쏠림을 이벤트로 남긴다.
 * 호가는 상태 스냅샷이라 윈도우 집계가 필요 없어 Flink 를 거치지 않는다.
 */
@Service
public class OrderbookService {

    private static final Logger logger = LoggerFactory.getLogger(OrderbookService.class);
    // 호가는 초당 여러 번 바뀌므로 대시보드에는 종목당 1초에 한 번만 보낸다
    private static final long BROADCAST_INTERVAL_MILLIS = 1_000;

    private final ObjectMapper objectMapper;
    private final int topLevels;
    private final ImbalanceDetector detector;
    private final DetectionEventRepository eventRepository;
    private final StreamBroadcaster broadcaster;

    private final Map<String, OrderbookImbalance> latest = new ConcurrentHashMap<>();
    private final Map<String, Long> lastBroadcast = new ConcurrentHashMap<>();

    public OrderbookService(ObjectMapper objectMapper,
            @Value("${orderbook.top-levels:5}") int topLevels,
            @Value("${orderbook.threshold-ratio:3.0}") double thresholdRatio,
            @Value("${orderbook.hold-seconds:10}") int holdSeconds,
            @Value("${orderbook.cooldown-seconds:600}") int cooldownSeconds,
            DetectionEventRepository eventRepository, StreamBroadcaster broadcaster) {
        this.objectMapper = objectMapper;
        this.topLevels = topLevels;
        this.detector = new ImbalanceDetector(thresholdRatio, holdSeconds, cooldownSeconds);
        this.eventRepository = eventRepository;
        this.broadcaster = broadcaster;
    }

    public List<OrderbookImbalance> current() {
        return latest.values().stream().sorted(Comparator.comparing(OrderbookImbalance::code)).toList();
    }

    @KafkaListener(topics = "${kafka.orderbook-topic}", groupId = "${kafka.api-group-id}",
            properties = "auto.offset.reset=latest")
    public void onOrderbook(String json) {
        Orderbook book;
        try {
            book = objectMapper.readValue(json, Orderbook.class);
        } catch (JsonProcessingException e) {
            logger.warn("호가 메시지 파싱 실패: {}", json.length() > 200 ? json.substring(0, 200) : json);
            return;
        }
        if (book.code == null || !"orderbook".equals(book.type)) {
            return;
        }
        OrderbookImbalance snapshot = OrderbookImbalance.of(book, topLevels);
        latest.put(snapshot.code(), snapshot);
        broadcastThrottled(snapshot);
        detector.onSnapshot(snapshot).ifPresent(event -> {
            if (eventRepository.save(event)) {
                logger.info("감지 이벤트: {}", event.getMessage());
                broadcaster.publishEvent(event);
            }
        });
    }

    private void broadcastThrottled(OrderbookImbalance snapshot) {
        long now = System.currentTimeMillis();
        Long last = lastBroadcast.get(snapshot.code());
        if (last != null && now - last < BROADCAST_INTERVAL_MILLIS) {
            return;
        }
        lastBroadcast.put(snapshot.code(), now);
        broadcaster.publishOrderbook(snapshot);
    }
}
