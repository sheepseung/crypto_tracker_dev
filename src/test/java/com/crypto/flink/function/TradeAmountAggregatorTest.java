package com.crypto.flink.function;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.TradeIntensity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TradeAmountAggregatorTest {

    private final TradeAmountAggregator aggregator = new TradeAmountAggregator();

    @Test
    void 매수와_매도를_금액으로_나눠_누적한다() {
        TradeAmountAggregator.Accumulator acc = aggregator.createAccumulator();
        acc = aggregator.add(trade("BID", 100_000_000, 0.5, 1_000), acc);
        acc = aggregator.add(trade("ASK", 100_000_000, 0.2, 2_000), acc);
        acc = aggregator.add(trade("BID", 101_000_000, 0.1, 1_500), acc);

        assertEquals(60_100_000, acc.buyAmount, 1e-6);
        assertEquals(20_000_000, acc.sellAmount, 1e-6);
        assertEquals(0.6, acc.buyVolume, 1e-9);
        assertEquals(0.2, acc.sellVolume, 1e-9);
        assertEquals(3, acc.tradeCount);
        assertEquals(2_000, acc.lastTradeTimestamp);
    }

    @Test
    void 병합은_양쪽을_더하고_마지막_체결_시각은_큰_쪽을_쓴다() {
        TradeAmountAggregator.Accumulator a = aggregator.add(trade("BID", 10, 1, 5), aggregator.createAccumulator());
        TradeAmountAggregator.Accumulator b = aggregator.add(trade("ASK", 20, 1, 9), aggregator.createAccumulator());

        TradeAmountAggregator.Accumulator merged = aggregator.merge(a, b);

        assertEquals(10, merged.buyAmount, 1e-9);
        assertEquals(20, merged.sellAmount, 1e-9);
        assertEquals(2, merged.tradeCount);
        assertEquals(9, merged.lastTradeTimestamp);
    }

    @Test
    void 체결강도는_매수_나누기_매도이고_매도가_없으면_상한으로_막는다() {
        assertEquals(150.0, TradeIntensity.intensityOf(150, 100), 1e-9);
        assertEquals(100.0, TradeIntensity.intensityOf(0, 0), 1e-9);
        assertEquals(TradeIntensity.MAX_INTENSITY, TradeIntensity.intensityOf(1, 0), 1e-9);
        assertEquals(60.0, TradeIntensity.buyRatioOf(60, 40), 1e-9);
        assertEquals(50.0, TradeIntensity.buyRatioOf(0, 0), 1e-9);
    }

    private static CryptoTrade trade(String askBid, double price, double volume, long tradeTimestamp) {
        CryptoTrade trade = new CryptoTrade();
        trade.setType(CryptoTrade.TYPE_TRADE);
        trade.setCode("KRW-BTC");
        trade.setAskBid(askBid);
        trade.setTradePrice(price);
        trade.setTradeVolume(volume);
        trade.setTradeTimestamp(tradeTimestamp);
        return trade;
    }
}
