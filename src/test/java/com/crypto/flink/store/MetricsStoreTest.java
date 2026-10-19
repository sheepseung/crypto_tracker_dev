package com.crypto.flink.store;

import com.crypto.flink.dto.TradeIntensity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsStoreTest {

    private final MetricsStore store = new MetricsStore();

    @Test
    void 종목_구간별로_최신_윈도우만_남긴다() {
        store.put(intensity("KRW-BTC", 300, 1_000, 10));
        store.put(intensity("KRW-BTC", 300, 2_000, 20));
        store.put(intensity("KRW-BTC", 300, 1_500, 15));

        List<TradeIntensity> result = store.byWindow(300);
        assertEquals(1, result.size());
        assertEquals(2_000, result.get(0).getWindowEnd());
        assertEquals(20, result.get(0).getNetAmount(), 1e-9);
    }

    @Test
    void 구간_조회는_종목별로_하나씩_주고_없는_구간은_비어_있다() {
        store.put(intensity("KRW-BTC", 60, 1_000, 1));
        store.put(intensity("KRW-ETH", 60, 1_000, 2));
        store.put(intensity("KRW-ETH", 300, 1_000, 3));

        assertEquals(List.of("KRW-BTC", "KRW-ETH"),
                store.byWindow(60).stream().map(TradeIntensity::getCode).toList());
        assertEquals(1, store.byWindow(300).size());
        assertTrue(store.byWindow(900).isEmpty());
    }

    @Test
    void 종목_조회는_구간_오름차순_맵이고_없는_종목은_비어_있다() {
        store.put(intensity("KRW-DOGE", 900, 1_000, 1));
        store.put(intensity("KRW-DOGE", 60, 1_000, 2));

        Map<Integer, TradeIntensity> windows = store.byCode("KRW-DOGE").orElseThrow();
        assertEquals(List.of(60, 900), List.copyOf(windows.keySet()));
        assertTrue(store.byCode("KRW-XRP").isEmpty());
    }

    private static TradeIntensity intensity(String code, int windowSeconds, long windowEnd, double netAmount) {
        TradeIntensity t = new TradeIntensity();
        t.setCode(code);
        t.setWindowSeconds(windowSeconds);
        t.setWindowEnd(windowEnd);
        t.setNetAmount(netAmount);
        return t;
    }
}
