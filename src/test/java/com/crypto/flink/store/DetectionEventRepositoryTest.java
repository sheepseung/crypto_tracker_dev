package com.crypto.flink.store;

import com.crypto.flink.dto.DetectionEvent;
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
@Import(DetectionEventRepository.class)
class DetectionEventRepositoryTest {

    @Autowired
    private DetectionEventRepository repository;

    @Test
    void 같은_이벤트는_한_번만_저장된다() {
        DetectionEvent event = event(DetectionEvent.LARGE_TRADE, "KRW-BTC", 1_000);

        assertTrue(repository.save(event));
        assertFalse(repository.save(event));
        assertEquals(1, repository.find(null, null, null, 10).size());
    }

    @Test
    void 조건으로_거르고_최신순으로_준다() {
        repository.save(event(DetectionEvent.LARGE_TRADE, "KRW-BTC", 1_000));
        repository.save(event(DetectionEvent.PRICE_SURGE, "KRW-BTC", 2_000));
        repository.save(event(DetectionEvent.LARGE_TRADE, "KRW-ETH", 3_000));
        repository.save(event(DetectionEvent.LARGE_TRADE, "KRW-BTC", 4_000));

        List<DetectionEvent> btc = repository.find("KRW-BTC", null, null, 10);
        assertEquals(List.of(4_000L, 2_000L, 1_000L), btc.stream().map(DetectionEvent::getEventTime).toList());

        List<DetectionEvent> largeSince = repository.find(null, DetectionEvent.LARGE_TRADE, 2_000L, 10);
        assertEquals(List.of(4_000L, 3_000L), largeSince.stream().map(DetectionEvent::getEventTime).toList());

        assertEquals(2, repository.find(null, null, null, 2).size());
    }

    @Test
    void 종류에_없는_필드는_null_로_돌아온다() {
        repository.save(event(DetectionEvent.PRICE_DROP, "KRW-DOGE", 5_000));

        DetectionEvent loaded = repository.find("KRW-DOGE", null, null, 1).get(0);
        assertEquals(DetectionEvent.PRICE_DROP, loaded.getType());
        assertNull(loaded.getSide());
        assertNull(loaded.getAmount());
        assertNull(loaded.getTradeCount());
    }

    private static DetectionEvent event(String type, String code, long eventTime) {
        DetectionEvent e = new DetectionEvent();
        e.setType(type);
        e.setCode(code);
        e.setEventTime(eventTime);
        e.setEventTimeAt("2024-06-10T15:00:00+09:00");
        if (DetectionEvent.LARGE_TRADE.equals(type)) {
            e.setSide("BID");
            e.setAmount(350_000_000.0);
            e.setPrice(100_000_000.0);
            e.setTradeCount(1);
        } else {
            e.setPriceFrom(200.0);
            e.setPriceTo(195.0);
            e.setChangePercent(-2.5);
            e.setWindowSeconds(300);
        }
        e.setMessage(type + " " + code);
        return e;
    }
}
