package com.crypto.flink.orderbook;

import com.crypto.flink.dto.DetectionEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WallTrackerTest {

    private static final long T0 = 1_700_000_000_000L;
    private static final double EOK = 100_000_000;

    // 최소 벽 금액 1백만 원, 다른 호가 평균의 5배, 30초 이상 산 벽만, 최대치의 20% 아래면 소멸, 5분 쿨다운
    private final WallTracker tracker = new WallTracker(1_000_000, 5.0, 30, 0.2, 300);

    @Test
    void 큰_매도벽이_최우선이_아닌_채로_사라지면_허수_벽이다() {
        // 매도 호가 5개: 100,100,100,100 원 수준은 작고 104 에 2억 벽
        feed(T0, book(100, new double[]{0.01, 0.01, 0.01, 0.01, 2_000_000.0 / 104}, 104));
        feed(T0 + 10_000, book(100, new double[]{0.01, 0.01, 0.01, 0.01, 2_000_000.0 / 104}, 104));
        List<DetectionEvent> early = feed(T0 + 20_000, book(100, new double[]{0.01, 0.01, 0.01, 0.01, 0.0}, 104));
        assertTrue(early.isEmpty(), "30초 안 된 벽은 노이즈");

        feed(T0 + 30_000, book(100, new double[]{0.01, 0.01, 0.01, 0.01, 2_000_000.0 / 104}, 104));
        feed(T0 + 70_000, book(100, new double[]{0.01, 0.01, 0.01, 0.01, 2_000_000.0 / 104}, 104));
        List<DetectionEvent> gone = feed(T0 + 71_000, book(100, new double[]{0.01, 0.01, 0.01, 0.01, 0.0}, 104));

        assertEquals(1, gone.size());
        DetectionEvent e = gone.get(0);
        assertEquals(DetectionEvent.WALL_PULLED, e.getType());
        assertEquals("ASK", e.getSide());
        assertEquals(104.0, e.getPrice(), 1e-9);
        assertEquals(2_000_000.0, e.getAmount(), 1e-6);
        assertEquals(41, e.getWindowSeconds());
        assertEquals(4.0, e.getChangePercent(), 1e-9);
    }

    @Test
    void 최우선_호가의_벽이_사라지면_벽_뚫김이고_쿨다운_안에는_다시_내지_않는다() {
        feed(T0, book(100, new double[]{2_000_000.0 / 100, 0.01, 0.01, 0.01, 0.01}, 100));
        feed(T0 + 40_000, book(100, new double[]{2_000_000.0 / 100, 0.01, 0.01, 0.01, 0.01}, 100));
        List<DetectionEvent> eaten = feed(T0 + 41_000, book(100, new double[]{0.001, 0.01, 0.01, 0.01, 0.01}, 100));
        assertEquals(DetectionEvent.WALL_EATEN, eaten.get(0).getType());

        feed(T0 + 50_000, book(100, new double[]{2_000_000.0 / 100, 0.01, 0.01, 0.01, 0.01}, 100));
        feed(T0 + 90_000, book(100, new double[]{2_000_000.0 / 100, 0.01, 0.01, 0.01, 0.01}, 100));
        assertTrue(feed(T0 + 91_000, book(100, new double[]{0.001, 0.01, 0.01, 0.01, 0.01}, 100)).isEmpty(), "쿨다운");
    }

    @Test
    void 벽이_살아_있으면_가장_큰_벽을_알려_준다() {
        feed(T0, book(100, new double[]{0.01, 0.01, 3_000_000.0 / 102, 0.01, 2_000_000.0 / 104}, 102));
        WallTracker.Wall biggest = tracker.biggestWall("KRW-BTC", "ASK").orElseThrow();
        assertEquals(102.0, biggest.price(), 1e-9);
        assertTrue(tracker.biggestWall("KRW-BTC", "BID").isEmpty());
    }

    private List<DetectionEvent> feed(long ts, Orderbook book) {
        book.timestamp = ts;
        return new ArrayList<>(tracker.onSnapshot(book));
    }

    /** 매도 호가 askBase+0..4 에 askSizes, 매수 호가는 전부 작게. */
    private static Orderbook book(double askBase, double[] askSizes, double ignored) {
        Orderbook b = new Orderbook();
        b.type = "orderbook";
        b.code = "KRW-BTC";
        List<Orderbook.Unit> units = new ArrayList<>();
        for (int i = 0; i < askSizes.length; i++) {
            Orderbook.Unit u = new Orderbook.Unit();
            u.askPrice = askBase + i;
            u.askSize = askSizes[i];
            u.bidPrice = askBase - 1 - i;
            u.bidSize = 0.01;
            units.add(u);
        }
        b.units = units;
        return b;
    }
}
