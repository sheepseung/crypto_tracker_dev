package com.crypto.flink.stream;

import com.crypto.flink.dto.TradeIntensity;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamBroadcasterTest {

    private final StreamBroadcaster broadcaster = new StreamBroadcaster();

    // 끊긴 연결이 빠지는 것은 서블릿 응답에 붙은 뒤 Spring 이 콜백을 부를 때만 일어나므로 실기동에서 확인한다
    @Test
    void 연결마다_따로_등록된다() {
        SseEmitter a = broadcaster.subscribe();
        SseEmitter b = broadcaster.subscribe();
        assertEquals(2, broadcaster.subscriberCount());
        assertEquals(true, a != b);
    }

    @Test
    void 아직_응답에_붙지_않은_연결에_보내도_실패하지_않는다() {
        broadcaster.subscribe();
        TradeIntensity intensity = new TradeIntensity();
        intensity.setCode("KRW-BTC");

        broadcaster.publishMetric(intensity);
        broadcaster.heartbeat();

        assertEquals(1, broadcaster.subscriberCount());
    }
}
