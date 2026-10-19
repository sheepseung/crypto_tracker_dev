package com.crypto.flink.function;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.DetectionEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PriceMoveDetectorTest {

    private static final long T0 = 1_700_000_000_000L;
    private static final long MIN = 60_000;

    private KeyedOneInputStreamOperatorTestHarness<String, CryptoTrade, DetectionEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new PriceMoveDetector(300, 2.0, 300), CryptoTrade::getCode, Types.STRING);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void 구간_안_가장_오래된_가격_대비_2퍼센트_오르면_급등을_낸다() throws Exception {
        price("KRW-BTC", 100_000_000, T0);
        price("KRW-BTC", 101_000_000, T0 + MIN);
        price("KRW-BTC", 102_000_000, T0 + 2 * MIN);

        List<DetectionEvent> events = harness.extractOutputValues();
        assertEquals(1, events.size());
        DetectionEvent surge = events.get(0);
        assertEquals(DetectionEvent.PRICE_SURGE, surge.getType());
        assertEquals(100_000_000, surge.getPriceFrom(), 1e-6);
        assertEquals(102_000_000, surge.getPriceTo(), 1e-6);
        assertEquals(2.0, surge.getChangePercent(), 1e-9);
        assertEquals(300, surge.getWindowSeconds());
    }

    @Test
    void 쿨다운_안에서는_같은_방향을_다시_내지_않지만_임계치만큼_더_오르면_낸다() throws Exception {
        price("KRW-BTC", 100_000_000, T0);
        price("KRW-BTC", 102_000_000, T0 + MIN);
        price("KRW-BTC", 102_500_000, T0 + 2 * MIN);
        price("KRW-BTC", 104_100_000, T0 + 3 * MIN);

        List<DetectionEvent> events = harness.extractOutputValues();
        assertEquals(2, events.size());
        assertEquals(102_000_000, events.get(0).getPriceTo(), 1e-6);
        assertEquals(104_100_000, events.get(1).getPriceTo(), 1e-6);
    }

    @Test
    void 급락은_방향이_달라_급등_쿨다운과_따로_본다() throws Exception {
        price("KRW-DOGE", 200, T0);
        price("KRW-DOGE", 204.1, T0 + MIN);
        price("KRW-DOGE", 195, T0 + 2 * MIN);

        List<DetectionEvent> events = harness.extractOutputValues();
        assertEquals(2, events.size());
        assertEquals(DetectionEvent.PRICE_SURGE, events.get(0).getType());
        assertEquals(DetectionEvent.PRICE_DROP, events.get(1).getType());
        assertTrue(events.get(1).getChangePercent() < -2.0);
    }

    @Test
    void 구간을_벗어난_가격은_기준에서_빠진다() throws Exception {
        price("KRW-BTC", 100_000_000, T0);
        price("KRW-BTC", 101_000_000, T0 + 4 * MIN);
        price("KRW-BTC", 102_500_000, T0 + 6 * MIN);

        List<DetectionEvent> events = harness.extractOutputValues();
        assertTrue(events.isEmpty(), "6분 뒤 가격은 4분 시점(1.01억) 대비 1.49% 라 급등이 아니다");
    }

    private void price(String code, double price, long ts) throws Exception {
        CryptoTrade trade = new CryptoTrade();
        trade.setType(CryptoTrade.TYPE_TRADE);
        trade.setCode(code);
        trade.setAskBid(CryptoTrade.BID);
        trade.setTradePrice(price);
        trade.setTradeVolume(0.01);
        trade.setTradeTimestamp(ts);
        harness.processElement(trade, ts);
    }
}
