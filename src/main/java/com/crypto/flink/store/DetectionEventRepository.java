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

    private static DetectionEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
        DetectionEvent event = new DetectionEvent();
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
