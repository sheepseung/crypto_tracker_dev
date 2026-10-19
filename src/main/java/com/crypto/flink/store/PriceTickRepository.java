package com.crypto.flink.store;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * 종목별 가격 틱. 이벤트 시점과 그 뒤 가격을 찾는 용도라 30초 해상도면 충분하다.
 */
@Repository
public class PriceTickRepository {

    private final JdbcTemplate jdbc;

    public PriceTickRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean save(String code, long ts, double price) {
        try {
            jdbc.update("INSERT INTO price_tick (code, ts, price) VALUES (?, ?, ?)", code, ts, price);
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    /** ts 에서 가장 가까운 틱 가격. 앞뒤 toleranceMillis 안에 없으면 비어 있다. */
    public Optional<Double> priceNear(String code, long ts, long toleranceMillis) {
        return jdbc.query("""
                SELECT price FROM price_tick
                WHERE code = ? AND ts BETWEEN ? AND ?
                ORDER BY ABS(ts - ?) ASC LIMIT 1
                """,
                rs -> rs.next() ? Optional.of(rs.getDouble("price")) : Optional.empty(),
                code, ts - toleranceMillis, ts + toleranceMillis, ts);
    }

    public int deleteOlderThan(long ts) {
        return jdbc.update("DELETE FROM price_tick WHERE ts < ?", ts);
    }
}
