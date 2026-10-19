package com.crypto.flink.store;

import com.crypto.flink.dto.TierAmount;
import com.crypto.flink.dto.TradeIntensity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 5분 경계에 떨어지는 체결강도 윈도우만 남긴다. 겹치지 않아 더하면 하루치가 된다.
 * 종목당 하루 288행이라 가볍다. 주체 구간별 금액은 JSON 한 컬럼(tiers)에 둔다. 구간 수가 설정이라 컬럼으로 못 박지 않는다.
 */
@Repository
public class IntensityBucketRepository {

    public static final int BUCKET_SECONDS = 300;
    private static final TypeReference<List<TierAmount>> TIER_LIST = new TypeReference<>() {
    };

    // 테스트 슬라이스에는 Jackson 빈이 없으므로 직접 만든다
    private final ObjectMapper json = new ObjectMapper();

    public record CodeTotals(String code, double buyAmount, double sellAmount, int tradeCount, int buckets) {
        public double totalAmount() {
            return buyAmount + sellAmount;
        }

        public double netAmount() {
            return buyAmount - sellAmount;
        }

        public double intensity() {
            return TradeIntensity.intensityOf(buyAmount, sellAmount);
        }
    }

    public record Bucket(String code, long windowEnd, double buyAmount, double sellAmount, int tradeCount,
            List<TierAmount> tiers, double openPrice, double closePrice) {
        public static Bucket of(TradeIntensity w) {
            return new Bucket(w.getCode(), w.getWindowEnd(), w.getBuyAmount(), w.getSellAmount(), w.getTradeCount(),
                    w.getTiers(), w.getOpenPrice(), w.getClosePrice());
        }

        public double priceChangePercent() {
            return openPrice > 0 ? (closePrice / openPrice - 1.0) * 100.0 : 0.0;
        }

        public double netAmount() {
            return buyAmount - sellAmount;
        }

        public double totalAmount() {
            return buyAmount + sellAmount;
        }

        public double buyRatio() {
            return TradeIntensity.buyRatioOf(buyAmount, sellAmount);
        }

        public double intensity() {
            return TradeIntensity.intensityOf(buyAmount, sellAmount);
        }

        /** 구간별 순매수. 구간이 없는 옛 행은 빈 배열. */
        public double[] tierNets() {
            return tiers.stream().mapToDouble(TierAmount::net).toArray();
        }
    }

    private final JdbcTemplate jdbc;

    public IntensityBucketRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** 5분 구간이고 끝이 5분 경계에 떨어지는 윈도우만 받는다. 그 외는 false. 같은 버킷이 다시 오면 무시한다. */
    public boolean saveIfAligned(TradeIntensity w) {
        if (w.getWindowSeconds() != BUCKET_SECONDS || w.getWindowEnd() % (BUCKET_SECONDS * 1000L) != 0) {
            return false;
        }
        try {
            jdbc.update("""
                    INSERT INTO intensity_bucket
                      (code, window_end, buy_amount, sell_amount, trade_count, tiers, open_price, close_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    w.getCode(), w.getWindowEnd(), w.getBuyAmount(), w.getSellAmount(), w.getTradeCount(),
                    toJson(w.getTiers()), w.getOpenPrice(), w.getClosePrice());
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    /** window_end 가 (from, to] 인 버킷을 종목·시간순으로. 그래프·백분위용. */
    public List<Bucket> bucketsBetween(long fromExclusive, long toInclusive) {
        return jdbc.query("""
                SELECT code, window_end, buy_amount, sell_amount, trade_count, tiers, open_price, close_price
                FROM intensity_bucket
                WHERE window_end > ? AND window_end <= ?
                ORDER BY code, window_end
                """,
                (rs, i) -> new Bucket(rs.getString("code"), rs.getLong("window_end"), rs.getDouble("buy_amount"),
                        rs.getDouble("sell_amount"), rs.getInt("trade_count"), fromJson(rs.getString("tiers")),
                        rs.getDouble("open_price"), rs.getDouble("close_price")),
                fromExclusive, toInclusive);
    }

    /** window_end 가 (from, to] 인 버킷을 종목별로 합친다. 자정에 끝나는 버킷은 그 전날 것이다. */
    public List<CodeTotals> totalsBetween(long fromExclusive, long toInclusive) {
        return jdbc.query("""
                SELECT code, SUM(buy_amount) AS buy, SUM(sell_amount) AS sell, SUM(trade_count) AS cnt, COUNT(*) AS buckets
                FROM intensity_bucket
                WHERE window_end > ? AND window_end <= ?
                GROUP BY code ORDER BY code
                """,
                (rs, i) -> new CodeTotals(rs.getString("code"), rs.getDouble("buy"), rs.getDouble("sell"),
                        rs.getInt("cnt"), rs.getInt("buckets")),
                fromExclusive, toInclusive);
    }

    private String toJson(List<TierAmount> tiers) {
        try {
            return json.writeValueAsString(tiers == null ? List.of() : tiers);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("tiers 직렬화 실패", e);
        }
    }

    private List<TierAmount> fromJson(String text) {
        if (text == null || text.isBlank()) {
            return List.of();
        }
        try {
            return json.readValue(text, TIER_LIST);
        } catch (JsonProcessingException e) {
            return List.of();
        }
    }
}
