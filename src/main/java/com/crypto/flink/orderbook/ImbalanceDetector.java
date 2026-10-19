package com.crypto.flink.orderbook;

import com.crypto.flink.dto.DetectionEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 호가 쏠림 판정. ratio 가 threshold 이상(매수벽)이거나 1/threshold 이하(매도벽)인 상태가
 * holdMillis 동안 끊기지 않고 이어졌을 때 한 번 낸다. 호가는 초당 여러 번 바뀌어 순간값으로 내면 노이즈뿐이다.
 * 같은 종목·같은 방향은 cooldownMillis 동안 다시 내지 않는다. 스레드 안전하지 않으므로 종목별 순서가 보장되는 소비자 스레드에서만 쓴다.
 */
public class ImbalanceDetector {

    private final double thresholdRatio;
    private final long holdMillis;
    private final long cooldownMillis;

    private final Map<String, String> currentSide = new HashMap<>();
    private final Map<String, Long> sideSince = new HashMap<>();
    private final Map<String, Long> lastEmitted = new HashMap<>();

    public ImbalanceDetector(double thresholdRatio, int holdSeconds, int cooldownSeconds) {
        this.thresholdRatio = thresholdRatio;
        this.holdMillis = holdSeconds * 1000L;
        this.cooldownMillis = cooldownSeconds * 1000L;
    }

    public Optional<DetectionEvent> onSnapshot(OrderbookImbalance snapshot) {
        String code = snapshot.code();
        long now = snapshot.timestamp();
        String side = sideOf(snapshot.ratio());

        if (side == null) {
            currentSide.remove(code);
            sideSince.remove(code);
            return Optional.empty();
        }
        if (!side.equals(currentSide.get(code))) {
            currentSide.put(code, side);
            sideSince.put(code, now);
            return Optional.empty();
        }
        if (now - sideSince.get(code) < holdMillis) {
            return Optional.empty();
        }
        String key = code + ":" + side;
        Long last = lastEmitted.get(key);
        if (last != null && now - last < cooldownMillis) {
            return Optional.empty();
        }
        lastEmitted.put(key, now);
        return Optional.of(DetectionEvent.orderbookImbalance(snapshot, side, (int) (holdMillis / 1000)));
    }

    private String sideOf(double ratio) {
        if (ratio >= thresholdRatio) {
            return "BID";
        }
        if (ratio <= 1.0 / thresholdRatio) {
            return "ASK";
        }
        return null;
    }
}
