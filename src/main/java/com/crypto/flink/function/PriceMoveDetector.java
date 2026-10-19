package com.crypto.flink.function;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.DetectionEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * F3 급등·급락 감지. 종목별로 window 안의 가장 오래된 체결가를 기준으로 현재가 변동률을 본다.
 * 같은 방향은 cooldown 동안 다시 내지 않되, 직전 이벤트 가격에서 threshold 만큼 더 움직이면 cooldown 중에도 낸다.
 * 시간은 모두 체결 시각 기준이다.
 */
public class PriceMoveDetector extends KeyedProcessFunction<String, CryptoTrade, DetectionEvent> {

    /** Flink 상태로 저장되므로 public 필드와 기본 생성자를 가진 POJO 로 둔다. */
    public static class PriceAt {
        public long ts;
        public double price;

        public PriceAt() {
        }

        public PriceAt(long ts, double price) {
            this.ts = ts;
            this.price = price;
        }
    }

    private static final String UP = "UP";
    private static final String DOWN = "DOWN";

    private final int windowSeconds;
    private final double thresholdPercent;
    private final long cooldownMillis;

    private transient ListState<PriceAt> prices;
    private transient MapState<String, Long> lastEmitted;
    private transient MapState<String, Double> lastEmittedPrice;

    public PriceMoveDetector(int windowSeconds, double thresholdPercent, int cooldownSeconds) {
        this.windowSeconds = windowSeconds;
        this.thresholdPercent = thresholdPercent;
        this.cooldownMillis = cooldownSeconds * 1000L;
    }

    @Override
    public void open(Configuration parameters) {
        prices = getRuntimeContext().getListState(new ListStateDescriptor<>("prices", PriceAt.class));
        lastEmitted = getRuntimeContext().getMapState(new MapStateDescriptor<>("last-emitted", Types.STRING, Types.LONG));
        lastEmittedPrice = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("last-emitted-price", Types.STRING, Types.DOUBLE));
    }

    @Override
    public void processElement(CryptoTrade trade, Context ctx, Collector<DetectionEvent> out) throws Exception {
        long now = trade.getTradeTimestamp();
        double price = trade.getTradePrice();

        long windowStart = now - windowSeconds * 1000L;
        List<PriceAt> kept = new ArrayList<>();
        for (PriceAt past : prices.get()) {
            if (past.ts > windowStart) {
                kept.add(past);
            }
        }
        Double reference = kept.isEmpty() ? null : kept.get(0).price;
        kept.add(new PriceAt(now, price));
        prices.update(kept);

        if (reference == null) {
            return;
        }
        double changePercent = (price - reference) / reference * 100.0;
        if (Math.abs(changePercent) < thresholdPercent) {
            return;
        }

        String direction = changePercent > 0 ? UP : DOWN;
        if (shouldEmit(direction, price, now)) {
            out.collect(DetectionEvent.priceMove(trade, reference, changePercent, windowSeconds));
            lastEmitted.put(direction, now);
            lastEmittedPrice.put(direction, price);
        }
    }

    private boolean shouldEmit(String direction, double price, long now) throws Exception {
        Long last = lastEmitted.get(direction);
        if (last == null || now - last >= cooldownMillis) {
            return true;
        }
        Double lastPrice = lastEmittedPrice.get(direction);
        double furtherMove = (price - lastPrice) / lastPrice * 100.0;
        return UP.equals(direction) ? furtherMove >= thresholdPercent : furtherMove <= -thresholdPercent;
    }
}
