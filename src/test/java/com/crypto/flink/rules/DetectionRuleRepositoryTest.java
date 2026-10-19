package com.crypto.flink.rules;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@JdbcTest
@Import(DetectionRuleRepository.class)
class DetectionRuleRepositoryTest {

    @Autowired
    private DetectionRuleRepository repository;

    @Test
    void 넣고_같은_code_로_다시_넣으면_바뀌고_지우면_없어진다() {
        repository.upsert(TestRules.rule("*", 3e8, 3e8, 60, 2.0, 300, 5.0, 300));
        repository.upsert(TestRules.rule("KRW-DOGE", 5e7, null, null, null, null, null, null));
        repository.upsert(TestRules.rule("KRW-DOGE", 7e7, null, null, 3.0, null, null, null));

        List<DetectionRule> all = repository.findAll();
        assertEquals(2, all.size());
        DetectionRule doge = all.stream().filter(r -> r.getCode().equals("KRW-DOGE")).findFirst().orElseThrow();
        assertEquals(7e7, doge.getLargeSingleAmount(), 1e-6);
        assertEquals(3.0, doge.getPriceThresholdPercent(), 1e-9);
        assertNull(doge.getLargeBurstAmount());

        assertTrue(repository.delete("KRW-DOGE"));
        assertFalse(repository.delete("KRW-DOGE"));
        assertEquals(1, repository.findAll().size());
    }
}
