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

class LargeTradeDetectorTest {

    private static final long T0 = 1_700_000_000_000L;
    private static final double EOK = 100_000_000;

    private KeyedOneInputStreamOperatorTestHarness<String, CryptoTrade, DetectionEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new LargeTradeDetector(3 * EOK, 3 * EOK, 10, 60), CryptoTrade::getCode, Types.STRING);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void 단일_체결이_임계치를_넘으면_한_번만_내고_쿨다운_안에는_다시_내지_않는다() throws Exception {
        harness.processElement(trade("KRW-BTC", CryptoTrade.BID, 100_000_000, 3.5, T0), T0);
        harness.processElement(trade("KRW-BTC", CryptoTrade.BID, 100_000_000, 4.0, T0 + 30_000), T0 + 30_000);
        harness.processElement(trade("KRW-BTC", CryptoTrade.BID, 100_000_000, 4.0, T0 + 61_000), T0 + 61_000);

        List<DetectionEvent> events = harness.extractOutputValues();
        assertEquals(2, events.size());
        assertEquals(DetectionEvent.LARGE_TRADE, events.get(0).getType());
        assertEquals(3.5 * EOK, events.get(0).getAmount(), 1e-6);
        assertEquals("BID", events.get(0).getSide());
        assertEquals(T0 + 61_000, events.get(1).getEventTime());
    }

    @Test
    void 작은_체결이_10초_안에_같은_방향으로_몰리면_집중_체결로_낸다() throws Exception {
        for (int i = 0; i < 4; i++) {
            harness.processElement(trade("KRW-ETH", CryptoTrade.ASK, 4_000_000, 20, T0 + i * 2_000), T0 + i * 2_000);
        }

        List<DetectionEvent> events = harness.extractOutputValues();
        assertEquals(1, events.size());
        DetectionEvent burst = events.get(0);
        assertEquals(DetectionEvent.LARGE_TRADE_BURST, burst.getType());
        assertEquals("ASK", burst.getSide());
        assertEquals(4, burst.getTradeCount());
        assertEquals(3.2 * EOK, burst.getAmount(), 1e-6);
        assertEquals(10, burst.getWindowSeconds());
    }

    @Test
    void 구간을_벗어난_체결은_합에서_빠진다() throws Exception {
        harness.processElement(trade("KRW-ETH", CryptoTrade.BID, 4_000_000, 50, T0), T0);
        harness.processElement(trade("KRW-ETH", CryptoTrade.BID, 4_000_000, 30, T0 + 11_000), T0 + 11_000);

        assertTrue(harness.extractOutputValues().isEmpty());
    }

    @Test
    void 방향이_다르면_합치지_않고_종목도_섞이지_않는다() throws Exception {
        harness.processElement(trade("KRW-ETH", CryptoTrade.BID, 4_000_000, 50, T0), T0);
        harness.processElement(trade("KRW-ETH", CryptoTrade.ASK, 4_000_000, 50, T0 + 1_000), T0 + 1_000);
        harness.processElement(trade("KRW-DOGE", CryptoTrade.BID, 200, 1_000_000, T0 + 2_000), T0 + 2_000);

        assertTrue(harness.extractOutputValues().isEmpty());
    }

    private static CryptoTrade trade(String code, String askBid, double price, double volume, long ts) {
        CryptoTrade trade = new CryptoTrade();
        trade.setType(CryptoTrade.TYPE_TRADE);
        trade.setCode(code);
        trade.setAskBid(askBid);
        trade.setTradePrice(price);
        trade.setTradeVolume(volume);
        trade.setTradeTimestamp(ts);
        return trade;
    }
}
