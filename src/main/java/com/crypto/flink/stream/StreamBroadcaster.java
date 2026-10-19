package com.crypto.flink.stream;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.dto.TradeIntensity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 대시보드로 열린 SSE 연결에 지표와 이벤트를 그대로 밀어준다.
 * 브라우저가 주기적으로 API 를 당기는 대신 결과가 올 때 바로 반영되게 한다.
 */
@Component
public class StreamBroadcaster {

    public static final String METRIC = "metric";
    public static final String EVENT = "event";
    public static final String SNAPSHOT = "snapshot";
    public static final String PREMIUM = "premium";
    public static final String ORDERBOOK = "orderbook";

    private static final Logger logger = LoggerFactory.getLogger(StreamBroadcaster.class);
    // 프록시나 브라우저가 조용한 연결을 끊지 않도록 주기적으로 빈 주석을 보낸다
    private static final long HEARTBEAT_MILLIS = 15_000;

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    /** 타임아웃 없는 연결을 만들어 등록한다. 끊기면 목록에서 빠진다. */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        emitters.add(emitter);
        return emitter;
    }

    public void sendSnapshot(SseEmitter emitter, Object snapshot) {
        send(emitter, SNAPSHOT, snapshot);
    }

    public void publishMetric(TradeIntensity intensity) {
        broadcast(METRIC, intensity);
    }

    public void publishEvent(DetectionEvent event) {
        broadcast(EVENT, event);
    }

    public void publishPremium(Object premium) {
        broadcast(PREMIUM, premium);
    }

    public void publishOrderbook(Object orderbook) {
        broadcast(ORDERBOOK, orderbook);
    }

    public int subscriberCount() {
        return emitters.size();
    }

    @Scheduled(fixedRate = HEARTBEAT_MILLIS)
    public void heartbeat() {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().comment("ping"));
            } catch (IOException | IllegalStateException e) {
                drop(emitter);
            }
        }
    }

    private void broadcast(String name, Object data) {
        for (SseEmitter emitter : emitters) {
            send(emitter, name, data);
        }
    }

    private void send(SseEmitter emitter, String name, Object data) {
        try {
            emitter.send(SseEmitter.event().name(name).data(data, MediaType.APPLICATION_JSON));
        } catch (IOException | IllegalStateException e) {
            drop(emitter);
        }
    }

    private void drop(SseEmitter emitter) {
        emitters.remove(emitter);
        try {
            emitter.complete();
        } catch (RuntimeException ignored) {
            // 이미 끊긴 연결이라 정리만 하면 된다
        }
        logger.debug("SSE 연결 정리, 남은 연결 {}", emitters.size());
    }
}
