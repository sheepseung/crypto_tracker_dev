package com.crypto.flink.rules;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class DetectionRulesTest {

    private final DetectionRule defaults = TestRules.rule("*", 3e8, 3e8, 60, 2.0, 300, 5.0, 300);

    @Test
    void 덮어쓰기가_없는_종목은_기본_규칙을_그대로_쓴다() {
        DetectionRules rules = DetectionRules.of(defaults);
        assertSame(defaults, rules.resolve("KRW-BTC"));
    }

    @Test
    void 덮어쓰기는_준_값만_바꾸고_나머지는_기본값을_따른다() {
        DetectionRule doge = TestRules.rule("KRW-DOGE", 5e7, null, null, 3.0, null, null, null);
        DetectionRules rules = new DetectionRules(defaults, Map.of("KRW-DOGE", doge));

        DetectionRule effective = rules.resolve("KRW-DOGE");
        assertEquals("KRW-DOGE", effective.getCode());
        assertEquals(5e7, effective.getLargeSingleAmount(), 1e-6);
        assertEquals(3e8, effective.getLargeBurstAmount(), 1e-6);
        assertEquals(3.0, effective.getPriceThresholdPercent(), 1e-9);
        assertEquals(300, effective.getPriceCooldownSeconds());
        assertEquals(5.0, effective.getVolumeMultiplier(), 1e-9);
    }

    @Test
    void 기본_규칙은_모든_값이_있어야_하고_덮어쓰기는_준_값만_검사한다() {
        assertNull(defaults.missingOrInvalidField(true));

        DetectionRule partial = TestRules.rule("KRW-ETH", null, null, null, 1.5, null, null, null);
        assertNull(partial.missingOrInvalidField(false));
        assertEquals("largeSingleAmount", partial.missingOrInvalidField(true));

        DetectionRule zero = TestRules.rule("KRW-ETH", null, null, 0, null, null, null, null);
        assertEquals("largeCooldownSeconds", zero.missingOrInvalidField(false));
    }
}
