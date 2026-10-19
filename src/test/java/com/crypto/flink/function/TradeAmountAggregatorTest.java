package com.crypto.flink.function;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.TradeIntensity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TradeAmountAggregatorTest {

    private final TradeAmountAggregator aggregator = new TradeAmountAggregator(
            List.of("개미", "소형", "중형", "큰손", "고래"), List.of(100_000.0, 1_000_000.0, 10_000_000.0, 100_000_000.0));

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
        // 시가는 체결 시각이 가장 이른 것(1_000), 종가는 가장 늦은 것(2_000)
        assertEquals(100_000_000, acc.openPrice, 1e-6);
        assertEquals(100_000_000, acc.closePrice, 1e-6);
    }

    @Test
    void 시가와_종가는_체결_시각_순서로_고르고_병합도_같다() {
        TradeAmountAggregator.Accumulator a = aggregator.add(trade("BID", 110, 1, 50), aggregator.createAccumulator());
        TradeAmountAggregator.Accumulator b = aggregator.add(trade("BID", 90, 1, 10), aggregator.createAccumulator());
        b = aggregator.add(trade("ASK", 95, 1, 70), b);

        TradeAmountAggregator.Accumulator merged = aggregator.merge(a, b);
        assertEquals(90, merged.openPrice, 1e-9);
        assertEquals(95, merged.closePrice, 1e-9);
        assertEquals(10, merged.openTimestamp);
        assertEquals(70, merged.lastTradeTimestamp);
    }

    @Test
    void 체결_금액으로_다섯_구간을_나눠_매수_매도_건수를_따로_쌓는다() {
        TradeAmountAggregator.Accumulator acc = aggregator.createAccumulator();
        acc = aggregator.add(trade("BID", 100_000_000, 2.0, 1), acc);    // 2억 → 고래
        acc = aggregator.add(trade("ASK", 100_000_000, 0.5, 2), acc);    // 5천만 → 큰손
        acc = aggregator.add(trade("BID", 1_000_000, 5.0, 3), acc);      // 500만 → 중형
        acc = aggregator.add(trade("ASK", 1_000_000, 0.5, 4), acc);      // 50만 → 소형
        acc = aggregator.add(trade("BID", 100_000, 0.999, 5), acc);      // 99,900 → 개미
        acc = aggregator.add(trade("BID", 100_000, 1.0, 6), acc);        // 10만 → 소형 (경계 포함)

        assertEquals(5, acc.tierNames.length);
        assertEquals("개미", acc.tierNames[0]);
        assertEquals(99_900, acc.tierBuy[0], 1e-6);
        assertEquals(1, acc.tierCount[0]);
        assertEquals(100_000, acc.tierBuy[1], 1e-6);
        assertEquals(500_000, acc.tierSell[1], 1e-6);
        assertEquals(5_000_000, acc.tierBuy[2], 1e-6);
        assertEquals(50_000_000, acc.tierSell[3], 1e-6);
        assertEquals(200_000_000, acc.tierBuy[4], 1e-6);
        assertEquals(100_000_000.0, acc.tierMins[4], 1e-6);
        double tierBuy = java.util.Arrays.stream(acc.tierBuy).sum();
        assertEquals(acc.buyAmount, tierBuy, 1e-6);
        assertEquals(4, aggregator.tierIndex(150_000_000));
        assertEquals(0, aggregator.tierIndex(1));
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
