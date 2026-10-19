package com.crypto.flink.orderbook;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.DetectionEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * G3 호가 벽 추적. 스냅샷을 이어 보며 한 가격대의 잔량이 같은 쪽 다른 호가 평균의 ratio 배 이상이고
 * minAmount(KRW) 이상이면 벽으로 본다. 벽이 minLifetime 넘게 살다가 잔량이 최대치의 goneRatio 아래로 떨어지면
 * 그 순간 최우선 호가였으면 체결로 소진된 "벽 뚫김"(WALL_EATEN), 아니었으면 취소된 "허수 벽"(WALL_PULLED) 으로 낸다.
 * 스냅샷만으로 취소와 체결을 완전히 가를 수는 없으므로 최우선 여부로 근사한다.
 * 스레드 안전하지 않다. 종목별 순서가 보장되는 소비자 스레드에서만 쓴다.
 */
public class WallTracker {

    public record Wall(String side, double price, double amount, double maxAmount, long firstSeen, long lastSeen) {
        public long ageMillis(long now) {
            return now - firstSeen;
        }
    }

    private final double minAmount;
    private final double ratio;
    private final long minLifetimeMillis;
    private final double goneRatio;
    private final long cooldownMillis;

    // key: code|side|price
    private final Map<String, Wall> walls = new HashMap<>();
    private final Map<String, Long> lastEmitted = new HashMap<>();

    public WallTracker(double minAmount, double ratio, int minLifetimeSeconds, double goneRatio, int cooldownSeconds) {
        this.minAmount = minAmount;
        this.ratio = ratio;
        this.minLifetimeMillis = minLifetimeSeconds * 1000L;
        this.goneRatio = goneRatio;
        this.cooldownMillis = cooldownSeconds * 1000L;
    }

    public List<DetectionEvent> onSnapshot(Orderbook book) {
        List<DetectionEvent> events = new ArrayList<>();
        if (book.units.isEmpty()) {
            return events;
        }
        long now = book.timestamp;
        double bestBid = book.units.get(0).bidPrice;
        double bestAsk = book.units.get(0).askPrice;

        Map<Double, Double> bidAmounts = new HashMap<>();
        Map<Double, Double> askAmounts = new HashMap<>();
        for (Orderbook.Unit u : book.units) {
            bidAmounts.put(u.bidPrice, u.bidPrice * u.bidSize);
            askAmounts.put(u.askPrice, u.askPrice * u.askSize);
        }

        track(book.code, CryptoTrade.BID, bidAmounts, now);
        track(book.code, CryptoTrade.ASK, askAmounts, now);

        // 사라졌거나 쪼그라든 벽을 판정한다
        List<String> gone = new ArrayList<>();
        for (Map.Entry<String, Wall> entry : walls.entrySet()) {
            Wall w = entry.getValue();
            if (!entry.getKey().startsWith(book.code + "|")) {
                continue;
            }
            Map<Double, Double> amounts = CryptoTrade.BID.equals(w.side()) ? bidAmounts : askAmounts;
            double current = amounts.getOrDefault(w.price(), 0.0);
            if (current >= w.maxAmount() * goneRatio) {
                continue;
            }
            gone.add(entry.getKey());
            if (w.ageMillis(now) < minLifetimeMillis) {
                continue;
            }
            boolean wasBest = CryptoTrade.BID.equals(w.side()) ? w.price() >= bestBid : w.price() <= bestAsk;
            String type = wasBest ? DetectionEvent.WALL_EATEN : DetectionEvent.WALL_PULLED;
            String key = book.code + "|" + type + "|" + w.side();
            Long last = lastEmitted.get(key);
            if (last != null && now - last < cooldownMillis) {
                continue;
            }
            lastEmitted.put(key, now);
            double best = CryptoTrade.BID.equals(w.side()) ? bestBid : bestAsk;
            events.add(DetectionEvent.wall(type, book.code, w, best, now));
        }
        gone.forEach(walls::remove);
        return events;
    }

    /** 종목의 현재 벽 중 각 방향에서 가장 큰 것. */
    public Optional<Wall> biggestWall(String code, String side) {
        return walls.entrySet().stream()
                .filter(e -> e.getKey().startsWith(code + "|" + side + "|"))
                .map(Map.Entry::getValue)
                .max(java.util.Comparator.comparingDouble(Wall::amount));
    }

    private void track(String code, String side, Map<Double, Double> amounts, long now) {
        double total = amounts.values().stream().mapToDouble(Double::doubleValue).sum();
        int n = amounts.size();
        for (Map.Entry<Double, Double> e : amounts.entrySet()) {
            double amount = e.getValue();
            // 자기를 뺀 나머지 호가 평균과 비교한다. 호가가 하나뿐이면 비교 대상이 없다
            double othersAvg = n > 1 ? (total - amount) / (n - 1) : 0;
            boolean isWall = amount >= minAmount && n > 1 && amount >= othersAvg * ratio;
            String key = code + "|" + side + "|" + e.getKey();
            Wall existing = walls.get(key);
            if (isWall) {
                walls.put(key, existing == null
                        ? new Wall(side, e.getKey(), amount, amount, now, now)
                        : new Wall(side, e.getKey(), amount, Math.max(existing.maxAmount(), amount), existing.firstSeen(), now));
            } else if (existing != null && amount >= existing.maxAmount() * goneRatio) {
                // 벽 기준에는 못 미치지만 아직 많이 남아 있으면 계속 지켜본다
                walls.put(key, new Wall(side, e.getKey(), amount, existing.maxAmount(), existing.firstSeen(), now));
            }
        }
    }
}
