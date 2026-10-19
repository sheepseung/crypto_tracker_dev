package com.crypto.flink.summary;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.store.DetectionEventRepository;
import com.crypto.flink.store.IntensityBucketRepository;
import com.crypto.flink.stream.StreamBroadcaster;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@JdbcTest
@Import({IntensityBucketRepository.class, DetectionEventRepository.class, StreamBroadcaster.class,
        DailySummaryService.class})
class DailySummaryServiceTest {

    private static final LocalDate DAY = LocalDate.of(2026, 10, 19);

    @Autowired
    private IntensityBucketRepository buckets;
    @Autowired
    private DetectionEventRepository events;
    @Autowired
    private DailySummaryService service;

    @Test
    void 경계_윈도우만_저장하고_같은_버킷은_한_번만_남긴다() {
        long midnight = DAY.atStartOfDay(DailySummaryService.KST).toInstant().toEpochMilli();

        assertTrue(buckets.saveIfAligned(window("KRW-BTC", 300, midnight + 300_000, 10, 5, 3)));
        assertFalse(buckets.saveIfAligned(window("KRW-BTC", 300, midnight + 300_000, 99, 99, 9)), "중복");
        assertFalse(buckets.saveIfAligned(window("KRW-BTC", 300, midnight + 305_000, 1, 1, 1)), "경계 아님");
        assertFalse(buckets.saveIfAligned(window("KRW-BTC", 60, midnight + 600_000, 1, 1, 1)), "1분 윈도우");

        assertEquals(1, buckets.totalsBetween(midnight, midnight + 86_400_000).size());

        IntensityBucketRepository.Bucket b = buckets.bucketsBetween(midnight, midnight + 86_400_000).get(0);
        assertEquals(midnight + 300_000, b.windowEnd());
        assertEquals(5, b.netAmount(), 1e-9);
        assertEquals(15, b.totalAmount(), 1e-9);
        assertEquals(200.0, b.intensity(), 1e-9);
    }

    @Test
    void 하루치_금액과_이벤트_수를_모으고_자정_버킷은_전날로_센다() {
        long midnight = DAY.atStartOfDay(DailySummaryService.KST).toInstant().toEpochMilli();
        long nextMidnight = midnight + 86_400_000;

        buckets.saveIfAligned(window("KRW-BTC", 300, midnight, 1_000, 1_000, 1));          // 전날 마지막 버킷
        buckets.saveIfAligned(window("KRW-BTC", 300, midnight + 300_000, 300, 100, 4));
        buckets.saveIfAligned(window("KRW-BTC", 300, nextMidnight, 500, 100, 2));         // 오늘 마지막 버킷
        buckets.saveIfAligned(window("KRW-ETH", 300, midnight + 600_000, 50, 900, 7));

        events.save(event(DetectionEvent.LARGE_TRADE, "KRW-BTC", midnight + 1_000));
        events.save(event(DetectionEvent.LARGE_TRADE, "KRW-BTC", midnight + 2_000));
        events.save(event(DetectionEvent.PRICE_DROP, "KRW-ETH", midnight + 3_000));
        events.save(event(DetectionEvent.PRICE_DROP, "KRW-ETH", nextMidnight));           // 다음 날

        DailySummary s = service.forDate(DAY);

        assertEquals("2026-10-19", s.date());
        assertEquals(2, s.codes().size());
        DailySummary.CodeSummary btc = s.codes().get(0);
        assertEquals("KRW-BTC", btc.code());
        assertEquals(800, btc.buyAmount(), 1e-9);
        assertEquals(200, btc.sellAmount(), 1e-9);
        assertEquals(600, btc.netAmount(), 1e-9);
        assertEquals(6, btc.tradeCount());
        assertEquals(2, btc.buckets());
        assertEquals(2.0 / 288, btc.coverage(), 1e-9);
        assertEquals("KRW-BTC", s.topNetBuyCode());
        assertEquals("KRW-BTC", s.topVolumeCode());
        assertEquals(3, s.eventCount());
        assertEquals(2, s.eventsByType().get("LARGE_TRADE"));
        assertEquals(1, s.eventsByCode().get("KRW-ETH"));
        assertFalse(s.complete());
    }

    private static TradeIntensity window(String code, int seconds, long end, double buy, double sell, int count) {
        TradeIntensity w = new TradeIntensity();
        w.setCode(code);
        w.setWindowSeconds(seconds);
        w.setWindowEnd(end);
        w.setBuyAmount(buy);
        w.setSellAmount(sell);
        w.setTradeCount(count);
        return w;
    }

    private static DetectionEvent event(String type, String code, long time) {
        DetectionEvent e = new DetectionEvent();
        e.setType(type);
        e.setCode(code);
        e.setEventTime(time);
        e.setEventTimeAt("2026-10-19T00:00:00+09:00");
        e.setMessage(type);
        return e;
    }
}
