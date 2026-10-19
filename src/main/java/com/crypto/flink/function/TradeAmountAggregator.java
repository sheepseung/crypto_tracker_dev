package com.crypto.flink.function;

import com.crypto.flink.dto.CryptoTrade;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 윈도우 안의 체결을 매수·매도 금액과 수량으로 누적한다.
 * 체결 한 건의 금액으로 주체를 고래·중형·개미로 나눠 구간별 매수·매도도 따로 쌓는다. 업비트는 합계만 주므로 이게 차별점이다.
 * 결과는 누적기 그대로 넘기고, 종목과 윈도우 시간은 ProcessWindowFunction 이 붙인다.
 */
public class TradeAmountAggregator
        implements AggregateFunction<CryptoTrade, TradeAmountAggregator.Accumulator, TradeAmountAggregator.Accumulator> {

    /** Flink 상태로 저장되므로 public 필드와 기본 생성자를 가진 POJO 로 둔다. */
    public static class Accumulator {
        public double buyAmount;
        public double sellAmount;
        public double buyVolume;
        public double sellVolume;
        public int tradeCount;
        public long lastTradeTimestamp;
        public double whaleBuy;
        public double whaleSell;
        public double midBuy;
        public double midSell;
        public double retailBuy;
        public double retailSell;
        // 윈도우 안 첫·마지막 체결가. 체결 시각으로 고르므로 순서가 어긋나도 맞다
        public double openPrice;
        public long openTimestamp = Long.MAX_VALUE;
        public double closePrice;

        public Accumulator() {
        }
    }

    private final double whaleMinAmount;
    private final double retailMaxAmount;

    public TradeAmountAggregator(double whaleMinAmount, double retailMaxAmount) {
        this.whaleMinAmount = whaleMinAmount;
        this.retailMaxAmount = retailMaxAmount;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(CryptoTrade trade, Accumulator acc) {
        double amount = trade.getTradeAmount();
        boolean bid = trade.isBid();
        if (bid) {
            acc.buyAmount += amount;
            acc.buyVolume += trade.getTradeVolume();
        } else {
            acc.sellAmount += amount;
            acc.sellVolume += trade.getTradeVolume();
        }
        if (amount >= whaleMinAmount) {
            if (bid) acc.whaleBuy += amount; else acc.whaleSell += amount;
        } else if (amount < retailMaxAmount) {
            if (bid) acc.retailBuy += amount; else acc.retailSell += amount;
        } else {
            if (bid) acc.midBuy += amount; else acc.midSell += amount;
        }
        acc.tradeCount++;
        long ts = trade.getTradeTimestamp();
        if (ts < acc.openTimestamp) {
            acc.openTimestamp = ts;
            acc.openPrice = trade.getTradePrice();
        }
        if (ts >= acc.lastTradeTimestamp) {
            acc.lastTradeTimestamp = ts;
            acc.closePrice = trade.getTradePrice();
        }
        return acc;
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Accumulator m = new Accumulator();
        m.buyAmount = a.buyAmount + b.buyAmount;
        m.sellAmount = a.sellAmount + b.sellAmount;
        m.buyVolume = a.buyVolume + b.buyVolume;
        m.sellVolume = a.sellVolume + b.sellVolume;
        m.tradeCount = a.tradeCount + b.tradeCount;
        m.lastTradeTimestamp = Math.max(a.lastTradeTimestamp, b.lastTradeTimestamp);
        m.whaleBuy = a.whaleBuy + b.whaleBuy;
        m.whaleSell = a.whaleSell + b.whaleSell;
        m.midBuy = a.midBuy + b.midBuy;
        m.midSell = a.midSell + b.midSell;
        m.retailBuy = a.retailBuy + b.retailBuy;
        m.retailSell = a.retailSell + b.retailSell;
        Accumulator earlier = a.openTimestamp <= b.openTimestamp ? a : b;
        m.openTimestamp = earlier.openTimestamp;
        m.openPrice = earlier.openPrice;
        m.closePrice = a.lastTradeTimestamp >= b.lastTradeTimestamp ? a.closePrice : b.closePrice;
        return m;
    }
}
