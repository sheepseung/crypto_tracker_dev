package com.crypto.flink.rules;

/** 감지기 테스트가 공통으로 쓰는 규칙 생성기. */
public final class TestRules {

    private TestRules() {
    }

    public static DetectionRule rule(String code, Double largeSingle, Double largeBurst, Integer largeCooldown,
            Double pricePercent, Integer priceCooldown, Double volumeMultiplier, Integer volumeCooldown) {
        DetectionRule r = new DetectionRule();
        r.setCode(code);
        r.setLargeSingleAmount(largeSingle);
        r.setLargeBurstAmount(largeBurst);
        r.setLargeCooldownSeconds(largeCooldown);
        r.setPriceThresholdPercent(pricePercent);
        r.setPriceCooldownSeconds(priceCooldown);
        r.setVolumeMultiplier(volumeMultiplier);
        r.setVolumeCooldownSeconds(volumeCooldown);
        return r;
    }

    /** 기획안 시작값: 단일·집중 3억, 60초 / ±2%, 5분 / 5배, 5분 */
    public static DetectionRules defaults() {
        return DetectionRules.of(rule(DetectionRule.DEFAULT_CODE,
                300_000_000.0, 300_000_000.0, 60, 2.0, 300, 5.0, 300));
    }
}
