package com.crypto.flink.rules;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 기본 규칙과 종목별 덮어쓰기를 묶어 종목에 맞는 유효 규칙을 돌려준다.
 * Job 을 띄울 때 스냅샷으로 감지기에 실리므로 바꾸려면 Job 을 다시 띄운다.
 */
public class DetectionRules implements Serializable {

    private final DetectionRule defaults;
    private final Map<String, DetectionRule> overrides;

    public DetectionRules(DetectionRule defaults, Map<String, DetectionRule> overrides) {
        this.defaults = defaults;
        this.overrides = new HashMap<>(overrides);
    }

    public static DetectionRules of(DetectionRule defaults) {
        return new DetectionRules(defaults, Map.of());
    }

    public DetectionRule resolve(String code) {
        DetectionRule override = overrides.get(code);
        if (override == null) {
            return defaults;
        }
        return defaults.overriddenBy(override);
    }

    public DetectionRule defaults() {
        return defaults;
    }

    public List<DetectionRule> overrides() {
        return overrides.values().stream()
                .sorted(java.util.Comparator.comparing(DetectionRule::getCode))
                .toList();
    }

    public Map<String, DetectionRule> overrideMap() {
        return Collections.unmodifiableMap(overrides);
    }
}
