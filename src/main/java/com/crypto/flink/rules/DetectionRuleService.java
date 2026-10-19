package com.crypto.flink.rules;

import com.crypto.flink.job.FlinkJobRunner;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * 감지 규칙을 읽고 바꾼다. 바꾸면 Flink Job 을 다시 띄워 새 임계치를 싣는다.
 * 설정 파일의 값은 DB 에 기본 규칙이 없을 때 한 번 심는 초깃값이다.
 */
@Service
public class DetectionRuleService implements RulesProvider {

    private static final Logger logger = LoggerFactory.getLogger(DetectionRuleService.class);

    private final DetectionRuleRepository repository;
    private final FlinkJobRunner jobRunner;
    private final DetectionRule seed;

    public DetectionRuleService(DetectionRuleRepository repository,
            // 러너도 이 서비스(감지 파이프라인 경유)를 쓰므로 순환 참조를 끊기 위해 늦게 받는다
            @Lazy FlinkJobRunner jobRunner,
            @Value("${detection.large-trade.single-amount:300000000}") double largeSingleAmount,
            @Value("${detection.large-trade.burst-amount:300000000}") double largeBurstAmount,
            @Value("${detection.large-trade.cooldown-seconds:60}") int largeCooldownSeconds,
            @Value("${detection.price-move.threshold-percent:2.0}") double priceThresholdPercent,
            @Value("${detection.price-move.cooldown-seconds:300}") int priceCooldownSeconds,
            @Value("${detection.volume-surge.multiplier:5.0}") double volumeMultiplier,
            @Value("${detection.volume-surge.cooldown-seconds:300}") int volumeCooldownSeconds) {
        this.repository = repository;
        this.jobRunner = jobRunner;
        this.seed = new DetectionRule();
        seed.setCode(DetectionRule.DEFAULT_CODE);
        seed.setLargeSingleAmount(largeSingleAmount);
        seed.setLargeBurstAmount(largeBurstAmount);
        seed.setLargeCooldownSeconds(largeCooldownSeconds);
        seed.setPriceThresholdPercent(priceThresholdPercent);
        seed.setPriceCooldownSeconds(priceCooldownSeconds);
        seed.setVolumeMultiplier(volumeMultiplier);
        seed.setVolumeCooldownSeconds(volumeCooldownSeconds);
    }

    @PostConstruct
    void seedDefaultsIfMissing() {
        boolean hasDefault = repository.findAll().stream()
                .anyMatch(rule -> DetectionRule.DEFAULT_CODE.equals(rule.getCode()));
        if (!hasDefault) {
            repository.upsert(seed);
            logger.info("기본 감지 규칙을 설정값으로 심음: {}", seed);
        }
    }

    @Override
    public DetectionRules current() {
        DetectionRule defaults = null;
        Map<String, DetectionRule> overrides = new HashMap<>();
        for (DetectionRule rule : repository.findAll()) {
            if (DetectionRule.DEFAULT_CODE.equals(rule.getCode())) {
                defaults = rule;
            } else {
                overrides.put(rule.getCode(), rule);
            }
        }
        return new DetectionRules(defaults != null ? defaults : seed, overrides);
    }

    /** 기본 규칙은 전체 교체, 종목 규칙은 준 값만 바꾼다. 둘 다 Job 을 다시 띄운다. */
    public DetectionRules update(String code, DetectionRule change) {
        boolean isDefault = DetectionRule.DEFAULT_CODE.equals(code);
        String problem = change.missingOrInvalidField(isDefault);
        if (problem != null) {
            throw new IllegalArgumentException("값이 비었거나 0 이하: " + problem);
        }
        DetectionRule toSave = change;
        if (!isDefault) {
            DetectionRule existing = current().overrideMap().get(code);
            toSave = existing != null ? existing.overriddenBy(change) : change;
        }
        toSave.setCode(code);
        repository.upsert(toSave);
        logger.info("감지 규칙 변경: {}", toSave);
        jobRunner.restart();
        return current();
    }

    public DetectionRules deleteOverride(String code) {
        if (DetectionRule.DEFAULT_CODE.equals(code)) {
            throw new IllegalArgumentException("기본 규칙은 지울 수 없다");
        }
        if (repository.delete(code)) {
            logger.info("감지 규칙 삭제: {}", code);
            jobRunner.restart();
        }
        return current();
    }
}
