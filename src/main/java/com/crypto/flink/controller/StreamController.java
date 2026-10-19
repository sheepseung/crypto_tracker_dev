package com.crypto.flink.controller;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.store.DetectionEventRepository;
import com.crypto.flink.store.MetricsStore;
import com.crypto.flink.stream.StreamBroadcaster;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/stream")
@Tag(name = "Stream", description = "대시보드용 SSE. 연결 직후 현재 상태(snapshot)를 보내고 이후 metric·event 를 밀어준다")
public class StreamController {

    private static final int SNAPSHOT_EVENTS = 50;

    private final StreamBroadcaster broadcaster;
    private final MetricsStore metricsStore;
    private final DetectionEventRepository eventRepository;

    public StreamController(StreamBroadcaster broadcaster, MetricsStore metricsStore,
            DetectionEventRepository eventRepository) {
        this.broadcaster = broadcaster;
        this.metricsStore = metricsStore;
        this.eventRepository = eventRepository;
    }

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "지표·이벤트 실시간 스트림")
    public SseEmitter stream() {
        SseEmitter emitter = broadcaster.subscribe();
        List<TradeIntensity> metrics = metricsStore.all();
        List<DetectionEvent> events = eventRepository.find(null, null, null, SNAPSHOT_EVENTS);
        broadcaster.sendSnapshot(emitter, Map.of("metrics", metrics, "events", events));
        return emitter;
    }
}
