package com.crypto.flink.function;

import com.crypto.flink.dto.CryptoTrade;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 윈도우 안의 체결을 매수·매도 금액과 수량으로 누적한다.
 * 체결 한 건의 금액으로 주체 구간(개미…고래)을 나눠 구간별 매수·매도·건수도 따로 쌓는다. 업비트는 합계만 주므로 이게 차별점이다.
 * 구간 값은 기본형 배열로 둔다. List 를 쓰면 Flink 가 Kryo 를 타는데 Java 17 에서는 JVM 옵션 없이 초기화에 실패한다.
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
        public String[] tierNames = new String[0];
        public double[] tierMins = new double[0];
        public double[] tierBuy = new double[0];
        public double[] tierSell = new double[0];
        public int[] tierCount = new int[0];
        // 윈도우 안 첫·마지막 체결가. 체결 시각으로 고르므로 순서가 어긋나도 맞다
        public double openPrice;
        public long openTimestamp = Long.MAX_VALUE;
        public double closePrice;

        public Accumulator() {
        }
    }

    // 구간 이름과 하한. 오름차순이고 첫 하한은 0 이다
    private final String[] tierNames;
    private final double[] tierMins;

    public TradeAmountAggregator(List<String> tierNames, List<Double> tierBounds) {
        if (tierBounds.size() + 1 != tierNames.size()) {
            throw new IllegalArgumentException("tiers.names 는 tiers.bounds 보다 하나 많아야 한다");
        }
        this.tierNames = tierNames.toArray(new String[0]);
        this.tierMins = new double[tierNames.size()];
        for (int i = 1; i < tierMins.length; i++) {
            tierMins[i] = tierBounds.get(i - 1);
        }
    }

    @Override
    public Accumulator createAccumulator() {
        Accumulator acc = new Accumulator();
        acc.tierNames = tierNames.clone();
        acc.tierMins = tierMins.clone();
        acc.tierBuy = new double[tierNames.length];
        acc.tierSell = new double[tierNames.length];
        acc.tierCount = new int[tierNames.length];
        return acc;
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
        int tier = tierIndex(amount);
        if (bid) acc.tierBuy[tier] += amount; else acc.tierSell[tier] += amount;
        acc.tierCount[tier]++;

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

    int tierIndex(double amount) {
        int idx = 0;
        for (int i = 1; i < tierMins.length; i++) {
            if (amount >= tierMins[i]) {
                idx = i;
            }
        }
        return idx;
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Accumulator m = createAccumulator();
        m.buyAmount = a.buyAmount + b.buyAmount;
        m.sellAmount = a.sellAmount + b.sellAmount;
        m.buyVolume = a.buyVolume + b.buyVolume;
        m.sellVolume = a.sellVolume + b.sellVolume;
        m.tradeCount = a.tradeCount + b.tradeCount;
        m.lastTradeTimestamp = Math.max(a.lastTradeTimestamp, b.lastTradeTimestamp);
        for (int i = 0; i < m.tierBuy.length; i++) {
            m.tierBuy[i] = a.tierBuy[i] + b.tierBuy[i];
            m.tierSell[i] = a.tierSell[i] + b.tierSell[i];
            m.tierCount[i] = a.tierCount[i] + b.tierCount[i];
        }
        Accumulator earlier = a.openTimestamp <= b.openTimestamp ? a : b;
        m.openTimestamp = earlier.openTimestamp;
        m.openPrice = earlier.openPrice;
        m.closePrice = a.lastTradeTimestamp >= b.lastTradeTimestamp ? a.closePrice : b.closePrice;
        return m;
    }

    @Override
    public String toString() {
        return "TradeAmountAggregator" + Arrays.toString(tierNames) + Arrays.toString(tierMins);
    }
}
