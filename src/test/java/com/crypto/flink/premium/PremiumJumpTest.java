package com.crypto.flink.premium;

import com.crypto.flink.dto.DetectionEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PremiumJumpTest {

    private static final long T0 = 1_700_000_000_000L;
    private static final long WINDOW = 300_000;

    @Test
    void 김프가_5분_새_임계치_넘게_변하면_이벤트이고_어느_쪽이_앞섰는지_적는다() {
        Deque<KimchiPremium> history = new ArrayDeque<>();
        // 5분 전: 업비트 1억, 바이낸스 환산 1억 → 김프 0%
        history.add(KimchiPremium.of("KRW-BTC", 100_000_000, 72_463.77, 1_380.0, "dunamu", T0));
        // 지금: 바이낸스가 1% 올라 환산 1.01억, 업비트는 그대로 → 김프 ≈ -0.99%
        KimchiPremium now = KimchiPremium.of("KRW-BTC", 100_000_000, 72_463.77 * 1.01, 1_380.0, "dunamu", T0 + WINDOW);
        history.add(now);

        Optional<DetectionEvent> event = KimchiPremiumService.jumpOf(history, now, WINDOW, 0.5);

        assertTrue(event.isPresent());
        DetectionEvent e = event.get();
        assertEquals(DetectionEvent.PREMIUM_JUMP, e.getType());
        assertEquals(-0.99, e.getChangePercent(), 0.01);
        assertEquals(0.0, e.getAmount(), 1e-9, "업비트 변화 0%");
        assertEquals(1.0, e.getRatio(), 1e-6, "바이낸스 변화 +1%");
        assertTrue(e.getMessage().contains("바이낸스가 앞서"));
    }

    @Test
    void 과거가_창의_절반도_안_되거나_변화가_작으면_내지_않는다() {
        Deque<KimchiPremium> history = new ArrayDeque<>();
        history.add(KimchiPremium.of("KRW-ETH", 100, 100, 1.0, "x", T0));
        KimchiPremium soon = KimchiPremium.of("KRW-ETH", 110, 100, 1.0, "x", T0 + 60_000);
        history.add(soon);
        assertTrue(KimchiPremiumService.jumpOf(history, soon, WINDOW, 0.5).isEmpty(), "1분밖에 안 됐다");

        KimchiPremium later = KimchiPremium.of("KRW-ETH", 100.3, 100, 1.0, "x", T0 + WINDOW);
        history.add(later);
        assertTrue(KimchiPremiumService.jumpOf(history, later, WINDOW, 0.5).isEmpty(), "0.3%p 는 임계치 미만");
    }
}
