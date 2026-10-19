package com.crypto.flink.scorecard;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.premium.UpbitLastPriceStore;
import com.crypto.flink.store.DetectionEventRepository;
import com.crypto.flink.store.PriceTickRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@JdbcTest
@Import({DetectionEventRepository.class, PriceTickRepository.class, ScorecardService.class})
class ScorecardTest {

    private static final long MIN = 60_000;

    @Autowired
    private DetectionEventRepository events;
    @Autowired
    private PriceTickRepository ticks;
    @Autowired
    private ScorecardService scorecard;

    @Test
    void 이벤트_뒤_가격으로_수익률을_채우고_성적표를_만든다() {
        long now = System.currentTimeMillis();
        long t0 = now - 70 * MIN;
        OutcomeFiller filler = new OutcomeFiller(new UpbitLastPriceStore(new ObjectMapper()), ticks, events,
                List.of("KRW-BTC"), 14);

        // 가격: 100 → 5분 뒤 101 → 15분 뒤 103 → 60분 뒤 98
        ticks.save("KRW-BTC", t0, 100);
        ticks.save("KRW-BTC", t0 + 5 * MIN, 101);
        ticks.save("KRW-BTC", t0 + 15 * MIN, 103);
        ticks.save("KRW-BTC", t0 + 60 * MIN, 98);

        events.save(event(DetectionEvent.LARGE_TRADE, "BID", t0, null));            // 가격 없음 → 틱으로
        events.save(event(DetectionEvent.PRICE_DROP, null, t0 + 1, 100.0));           // 가격 있음
        events.save(event(DetectionEvent.VOLUME_SURGE, null, now - 2 * MIN, 100.0)); // 아직 5분 안 됨

        List<DetectionEvent> pending = events.findOutcomePending(now - 5 * MIN, 10);
        assertEquals(2, pending.size(), "5분이 안 된 이벤트는 대상이 아니다");
        for (DetectionEvent e : pending) {
            assertTrue(filler.fill(e, now));
        }

        DetectionEvent large = events.find("KRW-BTC", DetectionEvent.LARGE_TRADE, null, 1).get(0);
        assertEquals(100.0, large.getPriceAt(), 1e-9);
        assertEquals(1.0, large.getReturn5m(), 1e-9);
        assertEquals(3.0, large.getReturn15m(), 1e-9);
        assertEquals(-2.0, large.getReturn60m(), 1e-9);

        List<ScorecardService.Row> rows = scorecard.scorecard(7, false);
        assertEquals(2, rows.size());
        ScorecardService.Row largeRow = rows.stream().filter(r -> r.type().equals(DetectionEvent.LARGE_TRADE)).findFirst().orElseThrow();
        assertTrue(largeRow.directional());
        assertEquals(100.0, largeRow.m5().hitRate(), 1e-9, "매수 신호 뒤 +1% 는 적중");
        assertEquals(0.0, largeRow.m60().hitRate(), 1e-9, "60분 뒤 -2% 는 빗나감");
        ScorecardService.Row dropRow = rows.stream().filter(r -> r.type().equals(DetectionEvent.PRICE_DROP)).findFirst().orElseThrow();
        assertEquals(0.0, dropRow.m5().hitRate(), 1e-9, "급락 신호인데 올랐으니 빗나감");
        assertEquals(100.0, dropRow.m60().hitRate(), 1e-9);
        assertEquals(1, dropRow.count());
    }

    @Test
    void 틱이_없으면_채우지_못하고_부분_지평만_채울_수도_있다() {
        long now = System.currentTimeMillis();
        long t0 = now - 20 * MIN;
        OutcomeFiller filler = new OutcomeFiller(new UpbitLastPriceStore(new ObjectMapper()), ticks, events,
                List.of("KRW-ETH"), 14);

        events.save(event(DetectionEvent.KIMCHI_PREMIUM, null, t0, 100.0));
        ticks.save("KRW-ETH", t0 + 5 * MIN, 102);

        DetectionEvent e = events.find("KRW-ETH", null, null, 1).get(0);
        assertFalse(filler.fill(e, now), "60분은 아직이라 완료가 아니다");
        DetectionEvent after = events.find("KRW-ETH", null, null, 1).get(0);
        assertEquals(2.0, after.getReturn5m(), 1e-9);
        assertNull(after.getReturn15m(), "15분 뒤 틱이 없다");
        assertNull(after.getReturn60m());

        ScorecardService.Row row = scorecard.scorecard(7, true).get(0);
        assertEquals("KRW-ETH", row.code());
        assertFalse(row.directional());
        assertNull(row.m5().hitRate());
        assertEquals(2.0, row.m5().avgAbsReturn(), 1e-9);
    }

    private static DetectionEvent event(String type, String side, long time, Double price) {
        DetectionEvent e = new DetectionEvent();
        e.setType(type);
        e.setCode(type.equals(DetectionEvent.KIMCHI_PREMIUM) ? "KRW-ETH" : "KRW-BTC");
        e.setEventTime(time);
        e.setEventTimeAt("2026-10-19T00:00:00+09:00");
        e.setSide(side);
        e.setPrice(price);
        e.setMessage(type);
        return e;
    }
}
