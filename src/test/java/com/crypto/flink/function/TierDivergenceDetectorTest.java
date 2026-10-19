package com.crypto.flink.function;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.dto.TradeIntensity;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TierDivergenceDetectorTest {

    private static final long T0 = 1_700_000_000_000L;
    private static final double EOK = 100_000_000;

    private KeyedOneInputStreamOperatorTestHarness<String, TradeIntensity, DetectionEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new TierDivergenceDetector(900, 0.3, 1 * EOK, 0.1 * EOK, 1800), TradeIntensity::getCode, Types.STRING);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void 고래가_사고_개미가_팔면_내고_쿨다운_안에는_다시_내지_않는다() throws Exception {
        feed(window("KRW-BTC", T0, 3 * EOK, 1 * EOK, 0.1 * EOK, 0.3 * EOK));
        feed(window("KRW-BTC", T0 + 60_000, 3 * EOK, 1 * EOK, 0.1 * EOK, 0.3 * EOK));

        List<DetectionEvent> events = harness.extractOutputValues();
        assertEquals(1, events.size());
        DetectionEvent e = events.get(0);
        assertEquals(DetectionEvent.TIER_DIVERGENCE, e.getType());
        assertEquals("BID", e.getSide());
        assertEquals(2 * EOK, e.getAmount(), 1e-6);
        assertEquals(-0.2 * EOK, e.getBaselineAmount(), 1e-6);
        assertEquals(0.5, e.getRatio(), 1e-9);
        assertEquals(-50.0, e.getChangePercent(), 1e-9);
        assertEquals(900, e.getWindowSeconds());
    }

    @Test
    void 같은_방향이거나_쏠림이_약하거나_표본이_작으면_내지_않는다() throws Exception {
        feed(window("KRW-ETH", T0, 3 * EOK, 1 * EOK, 0.3 * EOK, 0.1 * EOK));            // 둘 다 매수
        feed(window("KRW-ETH", T0 + 1, 1.2 * EOK, 1 * EOK, 0.1 * EOK, 0.3 * EOK));       // 고래 쏠림 9%
        feed(window("KRW-ETH", T0 + 2, 0.5 * EOK, 0.1 * EOK, 0.1 * EOK, 0.3 * EOK));     // 고래 표본 0.6억 < 1억
        feed(window("KRW-ETH", T0 + 3, 3 * EOK, 1 * EOK, 0.01 * EOK, 0.03 * EOK));       // 개미 표본 부족

        assertTrue(harness.extractOutputValues().isEmpty());
    }

    @Test
    void 다른_구간_길이의_윈도우는_무시한다() throws Exception {
        TradeIntensity w = window("KRW-DOGE", T0, 3 * EOK, 1 * EOK, 0.1 * EOK, 0.3 * EOK);
        w.setWindowSeconds(300);
        feed(w);
        assertTrue(harness.extractOutputValues().isEmpty());
    }

    private void feed(TradeIntensity w) throws Exception {
        harness.processElement(w, w.getLastTradeTimestamp());
    }

    private static TradeIntensity window(String code, long t, double whaleBuy, double whaleSell, double retailBuy,
            double retailSell) {
        TradeIntensity w = new TradeIntensity();
        w.setCode(code);
        w.setWindowSeconds(900);
        w.setWindowEnd(t);
        w.setLastTradeTimestamp(t);
        com.crypto.flink.dto.TierAmount retail = new com.crypto.flink.dto.TierAmount("개미", 0);
        retail.buyAmount = retailBuy;
        retail.sellAmount = retailSell;
        com.crypto.flink.dto.TierAmount whale = new com.crypto.flink.dto.TierAmount("고래", 100_000_000);
        whale.buyAmount = whaleBuy;
        whale.sellAmount = whaleSell;
        w.setTiers(List.of(retail, new com.crypto.flink.dto.TierAmount("중형", 1_000_000), whale));
        return w;
    }
}
