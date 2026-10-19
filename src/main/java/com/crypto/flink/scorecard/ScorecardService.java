package com.crypto.flink.scorecard;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.store.DetectionEventRepository;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * G2 신호 성적표. 성적이 채워진 이벤트를 종류(와 종목)별로 모아 건수, 평균 수익률, 방향 적중률을 낸다.
 * 방향이 있는 신호(매수벽·고래 매수·급등 등)는 기대 방향과 실제 수익률 부호가 같으면 적중이다.
 * 방향이 없는 신호(거래량 급증, 김프)는 적중률 대신 절대 변동폭 평균을 준다.
 */
@Service
public class ScorecardService {

    public record Horizon(int filled, Double avgReturn, Double hitRate, Double avgAbsReturn) {
    }

    public record Row(String type, String code, int count, Horizon m5, Horizon m15, Horizon m60, boolean directional) {
    }

    private final DetectionEventRepository events;

    public ScorecardService(DetectionEventRepository events) {
        this.events = events;
    }

    public List<Row> scorecard(int days, boolean byCode) {
        long from = System.currentTimeMillis() - days * 86_400_000L;
        Map<String, List<DetectionEvent>> groups = new LinkedHashMap<>();
        for (DetectionEvent e : events.findScored(from)) {
            String key = byCode ? e.getType() + "|" + e.getCode() : e.getType();
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(e);
        }
        List<Row> rows = new ArrayList<>();
        groups.forEach((key, list) -> {
            String type = list.get(0).getType();
            String code = byCode ? list.get(0).getCode() : null;
            rows.add(new Row(type, code, list.size(),
                    horizon(list, DetectionEvent::getReturn5m),
                    horizon(list, DetectionEvent::getReturn15m),
                    horizon(list, DetectionEvent::getReturn60m),
                    expectedDirection(list.get(0)) != 0));
        });
        rows.sort(Comparator.comparing(Row::type).thenComparing(r -> r.code() == null ? "" : r.code()));
        return rows;
    }

    /** 기대 방향: +1 상승, -1 하락, 0 없음. */
    static int expectedDirection(DetectionEvent e) {
        return switch (e.getType()) {
            case DetectionEvent.PRICE_SURGE -> 1;
            case DetectionEvent.PRICE_DROP -> -1;
            case DetectionEvent.LARGE_TRADE, DetectionEvent.LARGE_TRADE_BURST,
                 DetectionEvent.ORDERBOOK_IMBALANCE, DetectionEvent.TIER_DIVERGENCE ->
                    CryptoTrade.BID.equals(e.getSide()) ? 1 : CryptoTrade.ASK.equals(e.getSide()) ? -1 : 0;
            // 매도벽이 뚫리거나 거짓으로 드러나면 위로, 매수벽이면 아래로 기대한다
            case DetectionEvent.WALL_EATEN, DetectionEvent.WALL_PULLED ->
                    CryptoTrade.ASK.equals(e.getSide()) ? 1 : CryptoTrade.BID.equals(e.getSide()) ? -1 : 0;
            default -> 0;
        };
    }

    private static Horizon horizon(List<DetectionEvent> list, java.util.function.Function<DetectionEvent, Double> f) {
        int filled = 0, hits = 0, directional = 0;
        double sum = 0, sumAbs = 0;
        for (DetectionEvent e : list) {
            Double r = f.apply(e);
            if (r == null) {
                continue;
            }
            filled++;
            sum += r;
            sumAbs += Math.abs(r);
            int dir = expectedDirection(e);
            if (dir != 0) {
                directional++;
                if (Math.signum(r) == dir) {
                    hits++;
                }
            }
        }
        if (filled == 0) {
            return new Horizon(0, null, null, null);
        }
        return new Horizon(filled, sum / filled, directional == 0 ? null : 100.0 * hits / directional, sumAbs / filled);
    }
}
