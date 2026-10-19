package com.crypto.flink.service;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.job.FlinkJobRunner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TradeIntensityServiceTest {

    // 5초 슬라이드 경계에 맞춘 시각이라 윈도우 끝을 계산하기 쉽다
    private static final long BASE = 1_700_000_000_000L;

    @Test
    void 체결_시각_기준_60초_윈도우에_세_체결이_모두_들어간다() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<CryptoTrade> trades = FlinkJobRunner.withEventTime(env.fromElements(
                trade("KRW-BTC", CryptoTrade.BID, 100_000_000, 1.0, BASE),
                trade("KRW-BTC", CryptoTrade.ASK, 100_000_000, 0.5, BASE + 10_000),
                trade("KRW-BTC", CryptoTrade.BID, 100_000_000, 0.25, BASE + 20_000),
                trade("KRW-ETH", CryptoTrade.ASK, 4_000_000, 2.0, BASE + 20_000)));

        TradeIntensityService service = new TradeIntensityService(List.of(60), 5, true, null);

        List<TradeIntensity> results = new ArrayList<>();
        service.intensityStream(trades).executeAndCollect().forEachRemaining(results::add);

        TradeIntensity full = results.stream()
                .filter(r -> r.getCode().equals("KRW-BTC") && r.getWindowEnd() == BASE + 25_000)
                .findFirst()
                .orElseThrow();
        assertEquals(60, full.getWindowSeconds());
        assertEquals(BASE - 35_000, full.getWindowStart());
        assertEquals(3, full.getTradeCount());
        assertEquals(125_000_000, full.getBuyAmount(), 1e-6);
        assertEquals(50_000_000, full.getSellAmount(), 1e-6);
        assertEquals(75_000_000, full.getNetAmount(), 1e-6);
        assertEquals(250.0, full.getIntensity(), 1e-9);
        assertEquals(BASE + 20_000, full.getLastTradeTimestamp());

        TradeIntensity eth = results.stream()
                .filter(r -> r.getCode().equals("KRW-ETH") && r.getWindowEnd() == BASE + 25_000)
                .findFirst()
                .orElseThrow();
        assertEquals(1, eth.getTradeCount());
        assertEquals(8_000_000, eth.getSellAmount(), 1e-6);
        assertEquals(0.0, eth.getBuyRatio(), 1e-9);

        assertTrue(results.stream().allMatch(r -> r.getWindowSeconds() == 60));
    }

    private static CryptoTrade trade(String code, String askBid, double price, double volume, long tradeTimestamp) {
        CryptoTrade trade = new CryptoTrade();
        trade.setType(CryptoTrade.TYPE_TRADE);
        trade.setCode(code);
        trade.setAskBid(askBid);
        trade.setTradePrice(price);
        trade.setTradeVolume(volume);
        trade.setTradeTimestamp(tradeTimestamp);
        trade.setTimestamp(tradeTimestamp);
        return trade;
    }
}
