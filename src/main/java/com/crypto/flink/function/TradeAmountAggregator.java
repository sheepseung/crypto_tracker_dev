package com.crypto.flink.function;

import com.crypto.flink.dto.CryptoTrade;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 윈도우 안의 체결을 매수·매도 금액과 수량으로 누적한다.
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

        public Accumulator() {
        }
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(CryptoTrade trade, Accumulator acc) {
        if (trade.isBid()) {
            acc.buyAmount += trade.getTradeAmount();
            acc.buyVolume += trade.getTradeVolume();
        } else {
            acc.sellAmount += trade.getTradeAmount();
            acc.sellVolume += trade.getTradeVolume();
        }
        acc.tradeCount++;
        acc.lastTradeTimestamp = Math.max(acc.lastTradeTimestamp, trade.getTradeTimestamp());
        return acc;
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Accumulator merged = new Accumulator();
        merged.buyAmount = a.buyAmount + b.buyAmount;
        merged.sellAmount = a.sellAmount + b.sellAmount;
        merged.buyVolume = a.buyVolume + b.buyVolume;
        merged.sellVolume = a.sellVolume + b.sellVolume;
        merged.tradeCount = a.tradeCount + b.tradeCount;
        merged.lastTradeTimestamp = Math.max(a.lastTradeTimestamp, b.lastTradeTimestamp);
        return merged;
    }
}
