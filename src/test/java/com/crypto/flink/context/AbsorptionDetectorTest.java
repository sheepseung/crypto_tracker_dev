package com.crypto.flink.context;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.store.IntensityBucketRepository.Bucket;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbsorptionDetectorTest {

    private static final long T0 = 1_700_000_000_000L;

    private AbsorptionDetector detector(double netPct, double amountPct, int samples) {
        return new AbsorptionDetector(
                b -> Optional.of(new ContextService.CodeContext(b.code(), 50.0, amountPct, netPct, samples, 7)),
                95, 0.1, 1.0, 30, 100, 900);
    }

    @Test
    void 순매수가_상위권인데_가격이_안_움직이면_흡수다() {
        List<DetectionEvent> events = detector(97, 60, 300).onBucket(bucket(T0, 100, 100.05, 5e8, 1e8));
        assertEquals(1, events.size());
        DetectionEvent e = events.get(0);
        assertEquals(DetectionEvent.ABSORPTION, e.getType());
        assertEquals("BID", e.getSide());
        assertEquals(4e8, e.getAmount(), 1e-6);
        assertEquals(97.0, e.getRatio(), 1e-9);
        assertEquals(0.05, e.getChangePercent(), 1e-9);
    }

    @Test
    void 순매도_흡수는_ASK_이고_가격이_움직였으면_흡수가_아니다() {
        assertEquals("ASK", detector(3, 60, 300).onBucket(bucket(T0, 100, 100, 1e8, 5e8)).get(0).getSide());
        assertTrue(detector(97, 60, 300).onBucket(bucket(T0, 100, 100.5, 5e8, 1e8)).isEmpty());
    }

    @Test
    void 가격이_크게_움직였는데_거래대금이_하위권이면_유동성_공백이다() {
        List<DetectionEvent> events = detector(50, 20, 300).onBucket(bucket(T0, 100, 98.5, 1e7, 1.2e7));
        assertEquals(1, events.size());
        DetectionEvent e = events.get(0);
        assertEquals(DetectionEvent.LIQUIDITY_GAP, e.getType());
        assertEquals("ASK", e.getSide());
        assertEquals(-1.5, e.getChangePercent(), 1e-9);
        assertEquals(20.0, e.getRatio(), 1e-9);
    }

    @Test
    void 표본이_부족하거나_가격이_없으면_판정하지_않고_쿨다운_안에는_다시_내지_않는다() {
        assertTrue(detector(97, 60, 50).onBucket(bucket(T0, 100, 100, 5e8, 1e8)).isEmpty(), "표본 50 < 100");
        assertTrue(detector(97, 60, 300).onBucket(bucket(T0, 0, 0, 5e8, 1e8)).isEmpty(), "가격 없음");

        AbsorptionDetector d = detector(97, 60, 300);
        assertEquals(1, d.onBucket(bucket(T0, 100, 100, 5e8, 1e8)).size());
        assertTrue(d.onBucket(bucket(T0 + 300_000, 100, 100, 5e8, 1e8)).isEmpty(), "15분 쿨다운");
        assertEquals(1, d.onBucket(bucket(T0 + 900_000, 100, 100, 5e8, 1e8)).size());
    }

    private static Bucket bucket(long end, double open, double close, double buy, double sell) {
        return new Bucket("KRW-BTC", end, buy, sell, 10, 0, 0, 0, 0, 0, 0, open, close);
    }
}
