package com.crypto.flink.rules;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * 감지 규칙 저장소. 재시작 후에도 바꾼 임계치가 유지되도록 DB 에 둔다.
 */
@Repository
public class DetectionRuleRepository {

    private static final RowMapper<DetectionRule> ROW_MAPPER = DetectionRuleRepository::mapRow;

    private final JdbcTemplate jdbc;

    public DetectionRuleRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<DetectionRule> findAll() {
        return jdbc.query("SELECT * FROM detection_rule ORDER BY code", ROW_MAPPER);
    }

    /** 같은 code 가 있으면 통째로 바꾼다. 부분 갱신은 호출하는 쪽이 합쳐서 넘긴다. */
    public void upsert(DetectionRule rule) {
        int updated = jdbc.update("""
                UPDATE detection_rule SET
                  large_single_amount = ?, large_burst_amount = ?, large_cooldown_seconds = ?,
                  price_threshold_percent = ?, price_cooldown_seconds = ?,
                  volume_multiplier = ?, volume_cooldown_seconds = ?, updated_at = CURRENT_TIMESTAMP
                WHERE code = ?
                """,
                rule.getLargeSingleAmount(), rule.getLargeBurstAmount(), rule.getLargeCooldownSeconds(),
                rule.getPriceThresholdPercent(), rule.getPriceCooldownSeconds(),
                rule.getVolumeMultiplier(), rule.getVolumeCooldownSeconds(), rule.getCode());
        if (updated == 0) {
            jdbc.update("""
                    INSERT INTO detection_rule
                      (code, large_single_amount, large_burst_amount, large_cooldown_seconds,
                       price_threshold_percent, price_cooldown_seconds, volume_multiplier, volume_cooldown_seconds)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rule.getCode(), rule.getLargeSingleAmount(), rule.getLargeBurstAmount(),
                    rule.getLargeCooldownSeconds(), rule.getPriceThresholdPercent(), rule.getPriceCooldownSeconds(),
                    rule.getVolumeMultiplier(), rule.getVolumeCooldownSeconds());
        }
    }

    public boolean delete(String code) {
        return jdbc.update("DELETE FROM detection_rule WHERE code = ?", code) > 0;
    }

    private static DetectionRule mapRow(ResultSet rs, int rowNum) throws SQLException {
        DetectionRule rule = new DetectionRule();
        rule.setCode(rs.getString("code"));
        rule.setLargeSingleAmount(nullableDouble(rs, "large_single_amount"));
        rule.setLargeBurstAmount(nullableDouble(rs, "large_burst_amount"));
        rule.setLargeCooldownSeconds(nullableInt(rs, "large_cooldown_seconds"));
        rule.setPriceThresholdPercent(nullableDouble(rs, "price_threshold_percent"));
        rule.setPriceCooldownSeconds(nullableInt(rs, "price_cooldown_seconds"));
        rule.setVolumeMultiplier(nullableDouble(rs, "volume_multiplier"));
        rule.setVolumeCooldownSeconds(nullableInt(rs, "volume_cooldown_seconds"));
        return rule;
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
