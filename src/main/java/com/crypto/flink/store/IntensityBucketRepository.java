package com.crypto.flink.store;

import com.crypto.flink.dto.TradeIntensity;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 5분 경계에 떨어지는 체결강도 윈도우만 남긴다. 겹치지 않아 더하면 하루치가 된다.
 * 종목당 하루 288행이라 가볍다.
 */
@Repository
public class IntensityBucketRepository {

    public static final int BUCKET_SECONDS = 300;

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

    private final JdbcTemplate jdbc;

    public IntensityBucketRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** 5분 구간이고 끝이 5분 경계에 떨어지는 윈도우만 받는다. 그 외는 false. 같은 버킷이 다시 오면 무시한다. */
    public boolean saveIfAligned(TradeIntensity window) {
        if (window.getWindowSeconds() != BUCKET_SECONDS || window.getWindowEnd() % (BUCKET_SECONDS * 1000L) != 0) {
            return false;
        }
        try {
            jdbc.update("INSERT INTO intensity_bucket (code, window_end, buy_amount, sell_amount, trade_count) VALUES (?, ?, ?, ?, ?)",
                    window.getCode(), window.getWindowEnd(), window.getBuyAmount(), window.getSellAmount(),
                    window.getTradeCount());
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
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
}
