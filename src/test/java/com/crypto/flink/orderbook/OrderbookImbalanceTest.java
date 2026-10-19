package com.crypto.flink.orderbook;

import com.crypto.flink.dto.DetectionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderbookImbalanceTest {

    private static final long T0 = 1_700_000_000_000L;

    @Test
    void 업비트_호가_메시지에서_비율과_상위_호가_금액을_만든다() throws Exception {
        String json = "{\"type\":\"orderbook\",\"code\":\"KRW-BTC\",\"timestamp\":1700000000000,"
                + "\"total_ask_size\":2.0,\"total_bid_size\":6.0,\"orderbook_units\":["
                + "{\"ask_price\":100100000,\"bid_price\":100000000,\"ask_size\":0.5,\"bid_size\":1.0},"
                + "{\"ask_price\":100200000,\"bid_price\":99900000,\"ask_size\":0.5,\"bid_size\":2.0}],"
                + "\"stream_type\":\"REALTIME\",\"level\":0}";
        Orderbook book = new ObjectMapper().readValue(json, Orderbook.class);

        OrderbookImbalance s = OrderbookImbalance.of(book, 5);

        assertEquals(3.0, s.ratio(), 1e-9);
        assertEquals(100_000_000, s.bestBid(), 1e-6);
        assertEquals(100_100_000, s.bestAsk(), 1e-6);
        assertEquals(0.1, s.spreadPercent(), 1e-9);
        assertEquals(100_000_000 + 2 * 99_900_000, s.topBidAmount(), 1e-6);
        assertEquals(0.5 * 100_100_000 + 0.5 * 100_200_000, s.topAskAmount(), 1e-6);
        assertTrue(s.timestampIso().endsWith("+09:00"));
    }

    @Test
    void 매도_잔량이_없으면_비율을_상한으로_막는다() {
        assertEquals(OrderbookImbalance.MAX_RATIO, OrderbookImbalance.ratioOf(1, 0), 1e-9);
        assertEquals(1.0, OrderbookImbalance.ratioOf(0, 0), 1e-9);
        assertEquals(0.25, OrderbookImbalance.ratioOf(1, 4), 1e-9);
    }

    @Test
    void 쏠림이_hold_만큼_이어져야_내고_끊기면_처음부터_다시_센다() {
        ImbalanceDetector detector = new ImbalanceDetector(3.0, 10, 600);

        assertTrue(detector.onSnapshot(snapshot("KRW-BTC", 3.5, T0)).isEmpty());
        assertTrue(detector.onSnapshot(snapshot("KRW-BTC", 4.0, T0 + 5_000)).isEmpty());
        assertTrue(detector.onSnapshot(snapshot("KRW-BTC", 1.0, T0 + 7_000)).isEmpty(), "쏠림이 풀렸다");
        assertTrue(detector.onSnapshot(snapshot("KRW-BTC", 3.2, T0 + 8_000)).isEmpty(), "다시 시작");
        assertTrue(detector.onSnapshot(snapshot("KRW-BTC", 3.2, T0 + 17_000)).isEmpty(), "9초라 아직");

        Optional<DetectionEvent> event = detector.onSnapshot(snapshot("KRW-BTC", 3.3, T0 + 18_000));
        assertTrue(event.isPresent());
        assertEquals(DetectionEvent.ORDERBOOK_IMBALANCE, event.get().getType());
        assertEquals("BID", event.get().getSide());
        assertEquals(3.3, event.get().getRatio(), 1e-9);
        assertEquals(10, event.get().getWindowSeconds());

        assertTrue(detector.onSnapshot(snapshot("KRW-BTC", 3.3, T0 + 30_000)).isEmpty(), "쿨다운");
    }

    @Test
    void 매도벽은_역수_기준이고_방향별로_따로_센다() {
        ImbalanceDetector detector = new ImbalanceDetector(3.0, 10, 600);

        detector.onSnapshot(snapshot("KRW-ETH", 0.3, T0));
        Optional<DetectionEvent> ask = detector.onSnapshot(snapshot("KRW-ETH", 0.2, T0 + 10_000));
        assertEquals("ASK", ask.orElseThrow().getSide());

        detector.onSnapshot(snapshot("KRW-ETH", 4.0, T0 + 11_000));
        Optional<DetectionEvent> bid = detector.onSnapshot(snapshot("KRW-ETH", 4.0, T0 + 21_000));
        assertEquals("BID", bid.orElseThrow().getSide(), "반대 방향은 쿨다운과 무관하게 낸다");
    }

    private static OrderbookImbalance snapshot(String code, double ratio, long ts) {
        return new OrderbookImbalance(code, ratio, 1.0, ratio, 100, 101, 1.0, 1_000_000, 500_000, 5, ts,
                com.crypto.flink.util.KstTime.format(ts));
    }
}
