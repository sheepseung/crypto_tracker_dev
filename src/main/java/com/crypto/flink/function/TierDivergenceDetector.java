package com.crypto.flink.function;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.dto.TradeIntensity;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * G1 주체 엇갈림 감지. 한 윈도우에서 고래와 개미의 순매수 방향이 반대이고, 각자 자기 거래대금의 minTierRatio 이상
 * 한쪽으로 쏠렸을 때 낸다. 비율로 보므로 종목 규모와 무관하다. 표본이 너무 작으면(최소 금액 미만) 보지 않는다.
 * 같은 종목·같은 방향은 cooldown 동안 다시 내지 않는다. 시간은 윈도우의 마지막 체결 시각이다.
 */
public class TierDivergenceDetector extends KeyedProcessFunction<String, TradeIntensity, DetectionEvent> {

    private final int windowSeconds;
    private final double minTierRatio;
    private final double minWhaleAmount;
    private final double minRetailAmount;
    private final long cooldownMillis;

    private transient MapState<String, Long> lastEmitted;

    public TierDivergenceDetector(int windowSeconds, double minTierRatio, double minWhaleAmount,
            double minRetailAmount, int cooldownSeconds) {
        this.windowSeconds = windowSeconds;
        this.minTierRatio = minTierRatio;
        this.minWhaleAmount = minWhaleAmount;
        this.minRetailAmount = minRetailAmount;
        this.cooldownMillis = cooldownSeconds * 1000L;
    }

    @Override
    public void open(Configuration parameters) {
        lastEmitted = getRuntimeContext().getMapState(new MapStateDescriptor<>("last-emitted", Types.STRING, Types.LONG));
    }

    @Override
    public void processElement(TradeIntensity w, Context ctx, Collector<DetectionEvent> out) throws Exception {
        if (w.getWindowSeconds() != windowSeconds) {
            return;
        }
        double whaleTotal = w.getWhaleBuyAmount() + w.getWhaleSellAmount();
        double retailTotal = w.getRetailBuyAmount() + w.getRetailSellAmount();
        if (whaleTotal < minWhaleAmount || retailTotal < minRetailAmount) {
            return;
        }
        double whaleLean = w.getWhaleNetAmount() / whaleTotal;
        double retailLean = w.getRetailNetAmount() / retailTotal;
        if (Math.abs(whaleLean) < minTierRatio || Math.abs(retailLean) < minTierRatio) {
            return;
        }
        if (Math.signum(whaleLean) == Math.signum(retailLean)) {
            return;
        }
        String direction = whaleLean > 0 ? "WHALE_BUY" : "WHALE_SELL";
        long now = w.getLastTradeTimestamp();
        Long last = lastEmitted.get(direction);
        if (last != null && now - last < cooldownMillis) {
            return;
        }
        lastEmitted.put(direction, now);
        out.collect(DetectionEvent.tierDivergence(w, whaleLean, retailLean));
    }
}
