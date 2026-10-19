package com.crypto.flink.rules;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

/**
 * 한 종목(또는 기본값 "*")의 감지 임계치. 덮어쓰기 행은 바꾸고 싶은 값만 채우고 나머지는 null 로 둬 기본값을 따른다.
 * Flink 함수 안으로 실리므로 직렬화 가능해야 한다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DetectionRule implements Serializable {

    public static final String DEFAULT_CODE = "*";

    private String code;
    private Double largeSingleAmount;
    private Double largeBurstAmount;
    private Integer largeCooldownSeconds;
    private Double priceThresholdPercent;
    private Integer priceCooldownSeconds;
    private Double volumeMultiplier;
    private Integer volumeCooldownSeconds;

    public DetectionRule() {
    }

    /** 이 규칙 위에 덮어쓰기를 얹은 새 규칙. null 인 항목은 이 규칙 값을 유지한다. */
    public DetectionRule overriddenBy(DetectionRule override) {
        DetectionRule merged = new DetectionRule();
        merged.code = override.code;
        merged.largeSingleAmount = pick(override.largeSingleAmount, largeSingleAmount);
        merged.largeBurstAmount = pick(override.largeBurstAmount, largeBurstAmount);
        merged.largeCooldownSeconds = pick(override.largeCooldownSeconds, largeCooldownSeconds);
        merged.priceThresholdPercent = pick(override.priceThresholdPercent, priceThresholdPercent);
        merged.priceCooldownSeconds = pick(override.priceCooldownSeconds, priceCooldownSeconds);
        merged.volumeMultiplier = pick(override.volumeMultiplier, volumeMultiplier);
        merged.volumeCooldownSeconds = pick(override.volumeCooldownSeconds, volumeCooldownSeconds);
        return merged;
    }

    /** 기본 규칙은 모든 값이 있어야 한다. 비어 있거나 0 이하인 항목 이름을 돌려주고 문제 없으면 null. */
    public String missingOrInvalidField(boolean requireAll) {
        if (invalid(largeSingleAmount, requireAll)) return "largeSingleAmount";
        if (invalid(largeBurstAmount, requireAll)) return "largeBurstAmount";
        if (invalid(largeCooldownSeconds, requireAll)) return "largeCooldownSeconds";
        if (invalid(priceThresholdPercent, requireAll)) return "priceThresholdPercent";
        if (invalid(priceCooldownSeconds, requireAll)) return "priceCooldownSeconds";
        if (invalid(volumeMultiplier, requireAll)) return "volumeMultiplier";
        if (invalid(volumeCooldownSeconds, requireAll)) return "volumeCooldownSeconds";
        return null;
    }

    private static boolean invalid(Number value, boolean required) {
        if (value == null) {
            return required;
        }
        return value.doubleValue() <= 0;
    }

    private static <T> T pick(T preferred, T fallback) {
        return preferred != null ? preferred : fallback;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Double getLargeSingleAmount() {
        return largeSingleAmount;
    }

    public void setLargeSingleAmount(Double largeSingleAmount) {
        this.largeSingleAmount = largeSingleAmount;
    }

    public Double getLargeBurstAmount() {
        return largeBurstAmount;
    }

    public void setLargeBurstAmount(Double largeBurstAmount) {
        this.largeBurstAmount = largeBurstAmount;
    }

    public Integer getLargeCooldownSeconds() {
        return largeCooldownSeconds;
    }

    public void setLargeCooldownSeconds(Integer largeCooldownSeconds) {
        this.largeCooldownSeconds = largeCooldownSeconds;
    }

    public Double getPriceThresholdPercent() {
        return priceThresholdPercent;
    }

    public void setPriceThresholdPercent(Double priceThresholdPercent) {
        this.priceThresholdPercent = priceThresholdPercent;
    }

    public Integer getPriceCooldownSeconds() {
        return priceCooldownSeconds;
    }

    public void setPriceCooldownSeconds(Integer priceCooldownSeconds) {
        this.priceCooldownSeconds = priceCooldownSeconds;
    }

    public Double getVolumeMultiplier() {
        return volumeMultiplier;
    }

    public void setVolumeMultiplier(Double volumeMultiplier) {
        this.volumeMultiplier = volumeMultiplier;
    }

    public Integer getVolumeCooldownSeconds() {
        return volumeCooldownSeconds;
    }

    public void setVolumeCooldownSeconds(Integer volumeCooldownSeconds) {
        this.volumeCooldownSeconds = volumeCooldownSeconds;
    }

    @Override
    public String toString() {
        return "DetectionRule{" + code + ": single=" + largeSingleAmount + ", burst=" + largeBurstAmount
                + ", price=" + priceThresholdPercent + "%, volume=x" + volumeMultiplier + "}";
    }
}
