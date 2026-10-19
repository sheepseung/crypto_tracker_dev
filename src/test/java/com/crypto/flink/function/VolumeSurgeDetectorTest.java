package com.crypto.flink.function;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.rules.TestRules;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VolumeSurgeDetectorTest {

    // 5분 경계에 맞춘 시각
    private static final long T0 = 1_700_000_100_000L;
    private static final long BUCKET = 300_000;
    private static final double EOK = 100_000_000;

    private KeyedOneInputStreamOperatorTestHarness<String, TradeIntensity, DetectionEvent> harness;

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void 기준선을_채운_뒤_5배_이상이면_급증을_내고_쿨다운_안에는_다시_내지_않는다() throws Exception {
        open(Collections.nCopies(24, 1 * EOK));

        feed("KRW-DOGE", T0 + 5_000, 4.9 * EOK);
        feed("KRW-DOGE", T0 + 10_000, 5.0 * EOK);
        feed("KRW-DOGE", T0 + 15_000, 6.0 * EOK);
        feed("KRW-DOGE", T0 + 310_000, 7.0 * EOK);

        List<DetectionEvent> events = harness.extractOutputValues();
        assertEquals(2, events.size());
        DetectionEvent first = events.get(0);
        assertEquals(DetectionEvent.VOLUME_SURGE, first.getType());
        assertEquals(5.0 * EOK, first.getAmount(), 1e-6);
        assertEquals(1 * EOK, first.getBaselineAmount(), 1e-6);
        assertEquals(5.0, first.getRatio(), 1e-9);
        assertEquals(300, first.getWindowSeconds());
        // 이벤트 시각은 윈도우 끝이 아니라 마지막 체결 시각이다
        assertEquals(T0 + 309_999, events.get(1).getEventTime());
    }

    @Test
    void 기준선이_부족하면_판정하지_않고_정렬_버킷으로_채워진_뒤에_판정한다() throws Exception {
        open(List.of());

        // 5분 경계에 떨어지는 슬라이딩 출력만 기준선 버킷이 된다. 11개까지는 판정하지 않는다
        for (int i = 1; i <= 11; i++) {
            feed("KRW-BTC", T0 + i * BUCKET, 1 * EOK);
            feed("KRW-BTC", T0 + i * BUCKET + 5_000, 10 * EOK);
        }
        assertTrue(harness.extractOutputValues().isEmpty(), "12개째 버킷을 쌓기 전까지는 판정하지 않는다");

        feed("KRW-BTC", T0 + 12 * BUCKET, 1 * EOK);
        feed("KRW-BTC", T0 + 12 * BUCKET + 5_000, 10 * EOK);
        List<DetectionEvent> events = harness.extractOutputValues();
        assertEquals(1, events.size());
        assertEquals(1 * EOK, events.get(0).getBaselineAmount(), 1e-6);
        assertEquals(10.0, events.get(0).getRatio(), 1e-9);
    }

    @Test
    void 다른_구간_길이의_윈도우는_무시한다() throws Exception {
        open(Collections.nCopies(24, 1 * EOK));

        TradeIntensity oneMinute = window("KRW-ETH", 60, T0 + 5_000, 50 * EOK);
        harness.processElement(oneMinute, T0 + 5_000);

        assertTrue(harness.extractOutputValues().isEmpty());
    }

    private void open(List<Double> seed) throws Exception {
        BaselineSource source = (code, bucketSeconds, limit) -> seed;
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new VolumeSurgeDetector(300, 288, 12, TestRules.defaults(), source), TradeIntensity::getCode, Types.STRING);
    }

    private void feed(String code, long windowEnd, double totalAmount) throws Exception {
        harness.processElement(window(code, 300, windowEnd, totalAmount), windowEnd);
    }

    private static TradeIntensity window(String code, int windowSeconds, long windowEnd, double totalAmount) {
        TradeIntensity w = new TradeIntensity();
        w.setCode(code);
        w.setWindowSeconds(windowSeconds);
        w.setWindowStart(windowEnd - windowSeconds * 1000L);
        w.setWindowEnd(windowEnd);
        w.setTotalAmount(totalAmount);
        w.setTradeCount(10);
        w.setLastTradeTimestamp(windowEnd - 1);
        return w;
    }
}
