package com.crypto.flink.store;

import com.crypto.flink.dto.DetectionEvent;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 감지 이벤트 이력. 재시작 후에도 되짚어 볼 수 있도록 DB 에 남긴다.
 * 성적표(price_at, return_*)는 저장 뒤 OutcomeFiller 가 채운다.
 */
@Repository
public class DetectionEventRepository {

    private static final RowMapper<DetectionEvent> ROW_MAPPER = DetectionEventRepository::mapRow;

    private final JdbcTemplate jdbc;

    public DetectionEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** 같은 이벤트가 다시 와도(Kafka 재소비, Job 재시작) 한 번만 남긴다. */
    public boolean save(DetectionEvent event) {
        try {
            jdbc.update("""
                    INSERT INTO detection_event
                      (type, code, event_time, event_time_at, side, amount, price, trade_count,
                       window_seconds, price_from, price_to, change_percent, baseline_amount, ratio, message)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    event.getType(), event.getCode(), event.getEventTime(), event.getEventTimeAt(),
                    event.getSide(), event.getAmount(), event.getPrice(), event.getTradeCount(),
                    event.getWindowSeconds(), event.getPriceFrom(), event.getPriceTo(), event.getChangePercent(),
                    event.getBaselineAmount(), event.getRatio(), event.getMessage());
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    /** 조건에 맞는 이벤트를 최신순으로. null 인 조건은 무시한다. */
    public List<DetectionEvent> find(String code, String type, Long sinceEventTime, int limit) {
        StringBuilder sql = new StringBuilder("SELECT * FROM detection_event WHERE 1=1");
        List<Object> args = new ArrayList<>();
        if (code != null) {
            sql.append(" AND code = ?");
            args.add(code);
        }
        if (type != null) {
            sql.append(" AND type = ?");
            args.add(type);
        }
        if (sinceEventTime != null) {
            sql.append(" AND event_time >= ?");
            args.add(sinceEventTime);
        }
        sql.append(" ORDER BY event_time DESC, id DESC LIMIT ?");
        args.add(limit);
        return jdbc.query(sql.toString(), ROW_MAPPER, args.toArray());
    }

    public record TypeCodeCount(String type, String code, int count) {
    }

    /** event_time 이 [from, to) 인 이벤트를 종류·종목별로 센다. */
    public List<TypeCodeCount> countBetween(long fromInclusive, long toExclusive) {
        return jdbc.query("""
                SELECT type, code, COUNT(*) AS cnt FROM detection_event
                WHERE event_time >= ? AND event_time < ?
                GROUP BY type, code ORDER BY type, code
                """,
                (rs, i) -> new TypeCodeCount(rs.getString("type"), rs.getString("code"), rs.getInt("cnt")),
                fromInclusive, toExclusive);
    }

    /** 60분 수익률이 아직 없고 event_time 이 before 이전인 이벤트. 오래된 순. */
    public List<DetectionEvent> findOutcomePending(long before, int limit) {
        return jdbc.query("""
                SELECT * FROM detection_event
                WHERE return_60m IS NULL AND event_time <= ?
                ORDER BY event_time ASC LIMIT ?
                """, ROW_MAPPER, before, limit);
    }

    /** 채울 수 있는 값만 넣는다. null 은 기존 값을 유지한다. */
    public void updateOutcome(long id, Double priceAt, Double return5m, Double return15m, Double return60m) {
        jdbc.update("""
                UPDATE detection_event SET
                  price_at = COALESCE(?, price_at), return_5m = COALESCE(?, return_5m),
                  return_15m = COALESCE(?, return_15m), return_60m = COALESCE(?, return_60m)
                WHERE id = ?
                """, priceAt, return5m, return15m, return60m, id);
    }

    /** event_time 이 from 이후이고 성적이 하나라도 있는 이벤트. 성적표 집계용. */
    public List<DetectionEvent> findScored(long fromInclusive) {
        return jdbc.query("""
                SELECT * FROM detection_event
                WHERE event_time >= ? AND (return_5m IS NOT NULL OR return_15m IS NOT NULL OR return_60m IS NOT NULL)
                ORDER BY event_time DESC
                """, ROW_MAPPER, fromInclusive);
    }

    private static DetectionEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
        DetectionEvent event = new DetectionEvent();
        event.setId(rs.getLong("id"));
        event.setType(rs.getString("type"));
        event.setCode(rs.getString("code"));
        event.setEventTime(rs.getLong("event_time"));
        event.setEventTimeAt(rs.getString("event_time_at"));
        event.setSide(rs.getString("side"));
        event.setAmount(nullableDouble(rs, "amount"));
        event.setPrice(nullableDouble(rs, "price"));
        event.setTradeCount(nullableInt(rs, "trade_count"));
        event.setWindowSeconds(nullableInt(rs, "window_seconds"));
        event.setPriceFrom(nullableDouble(rs, "price_from"));
        event.setPriceTo(nullableDouble(rs, "price_to"));
        event.setChangePercent(nullableDouble(rs, "change_percent"));
        event.setBaselineAmount(nullableDouble(rs, "baseline_amount"));
        event.setRatio(nullableDouble(rs, "ratio"));
        event.setMessage(rs.getString("message"));
        event.setPriceAt(nullableDouble(rs, "price_at"));
        event.setReturn5m(nullableDouble(rs, "return_5m"));
        event.setReturn15m(nullableDouble(rs, "return_15m"));
        event.setReturn60m(nullableDouble(rs, "return_60m"));
        return event;
    }

    private static Double nullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    private static Integer nullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }
}
