package com.crypto.flink.dto;

import java.time.LocalDateTime;

/**
 * 실시간 체결강도(Trade Intensity) 데이터 전송 객체
 * 
 * 체결강도 = (총 매수 체결량 / 총 매도 체결량) * 100
 * - 100 이상: 매수 우위
 * - 100 미만: 매도 우위
 * - 150 이상: 강한 매수세
 * - 70 이하: 강한 매도세
 */
public class TradeIntensity {

    private String timestamp; // 계산 시점
    private String code; // 암호화폐 코드
    private String windowStart; // 윈도우 시작 시간
    private String windowEnd; // 윈도우 종료 시간
    private Integer windowSizeSeconds; // 윈도우 크기 (초)

    private Double totalBuyVolume; // 총 매수 체결량
    private Double totalSellVolume; // 총 매도 체결량
    private Double totalVolume; // 총 체결량
    private Double intensityScore; // 체결강도 점수 (100 기준)
    private Double intensityPercentage; // 체결강도 백분율

    private String marketSentiment; // 시장 심리 (BULLISH, BEARISH, NEUTRAL)
    private String intensityLevel; // 강도 레벨 (VERY_HIGH, HIGH, MEDIUM, LOW, VERY_LOW)
    private String trendDirection; // 트렌드 방향 (RISING, FALLING, STABLE)
    private Double trendStrength; // 트렌드 강도 (0.0 ~ 100.0)

    private String alertLevel; // 알림 레벨 (CRITICAL, HIGH, MEDIUM, LOW)
    private String alertMessage; // 알림 메시지
    private Boolean isAlertTriggered; // 알림 발생 여부

    // 기본 생성자
    public TradeIntensity() {
    }

    // 전체 파라미터 생성자
    public TradeIntensity(String timestamp, String code, String windowStart, String windowEnd,
            Integer windowSizeSeconds, Double totalBuyVolume, Double totalSellVolume,
            Double totalVolume, Double intensityScore, Double intensityPercentage,
            String marketSentiment, String intensityLevel, String trendDirection,
            Double trendStrength, String alertLevel, String alertMessage, Boolean isAlertTriggered) {
        this.timestamp = timestamp;
        this.code = code;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.windowSizeSeconds = windowSizeSeconds;
        this.totalBuyVolume = totalBuyVolume;
        this.totalSellVolume = totalSellVolume;
        this.totalVolume = totalVolume;
        this.intensityScore = intensityScore;
        this.intensityPercentage = intensityPercentage;
        this.marketSentiment = marketSentiment;
        this.intensityLevel = intensityLevel;
        this.trendDirection = trendDirection;
        this.trendStrength = trendStrength;
        this.alertLevel = alertLevel;
        this.alertMessage = alertMessage;
        this.isAlertTriggered = isAlertTriggered;
    }

    // Getter & Setter
    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Integer getWindowSizeSeconds() {
        return windowSizeSeconds;
    }

    public void setWindowSizeSeconds(Integer windowSizeSeconds) {
        this.windowSizeSeconds = windowSizeSeconds;
    }

    public Double getTotalBuyVolume() {
        return totalBuyVolume;
    }

    public void setTotalBuyVolume(Double totalBuyVolume) {
        this.totalBuyVolume = totalBuyVolume;
    }

    public Double getTotalSellVolume() {
        return totalSellVolume;
    }

    public void setTotalSellVolume(Double totalSellVolume) {
        this.totalSellVolume = totalSellVolume;
    }

    public Double getTotalVolume() {
        return totalVolume;
    }

    public void setTotalVolume(Double totalVolume) {
        this.totalVolume = totalVolume;
    }

    public Double getIntensityScore() {
        return intensityScore;
    }

    public void setIntensityScore(Double intensityScore) {
        this.intensityScore = intensityScore;
    }

    public Double getIntensityPercentage() {
        return intensityPercentage;
    }

    public void setIntensityPercentage(Double intensityPercentage) {
        this.intensityPercentage = intensityPercentage;
    }

    public String getMarketSentiment() {
        return marketSentiment;
    }

    public void setMarketSentiment(String marketSentiment) {
        this.marketSentiment = marketSentiment;
    }

    public String getIntensityLevel() {
        return intensityLevel;
    }

    public void setIntensityLevel(String intensityLevel) {
        this.intensityLevel = intensityLevel;
    }

    public String getTrendDirection() {
        return trendDirection;
    }

    public void setTrendDirection(String trendDirection) {
        this.trendDirection = trendDirection;
    }

    public Double getTrendStrength() {
        return trendStrength;
    }

    public void setTrendStrength(Double trendStrength) {
        this.trendStrength = trendStrength;
    }

    public String getAlertLevel() {
        return alertLevel;
    }

    public void setAlertLevel(String alertLevel) {
        this.alertLevel = alertLevel;
    }

    public String getAlertMessage() {
        return alertMessage;
    }

    public void setAlertMessage(String alertMessage) {
        this.alertMessage = alertMessage;
    }

    public Boolean getIsAlertTriggered() {
        return isAlertTriggered;
    }

    public void setIsAlertTriggered(Boolean isAlertTriggered) {
        this.isAlertTriggered = isAlertTriggered;
    }

    // 헬퍼 메서드
    public Double getNetVolume() {
        return totalBuyVolume - totalSellVolume;
    }

    public Double getBuyRatio() {
        return totalVolume > 0 ? totalBuyVolume / totalVolume : 0.0;
    }

    public Double getSellRatio() {
        return totalVolume > 0 ? totalSellVolume / totalVolume : 0.0;
    }

    public Boolean isBullish() {
        return "BULLISH".equals(marketSentiment);
    }

    public Boolean isBearish() {
        return "BEARISH".equals(marketSentiment);
    }

    public Boolean isNeutral() {
        return "NEUTRAL".equals(marketSentiment);
    }

    public String getIntensityDescription() {
        if (intensityScore >= 150)
            return "매우 강한 매수세";
        if (intensityScore >= 120)
            return "강한 매수세";
        if (intensityScore >= 110)
            return "약한 매수세";
        if (intensityScore >= 90)
            return "중립";
        if (intensityScore >= 80)
            return "약한 매도세";
        if (intensityScore >= 50)
            return "강한 매도세";
        return "매우 강한 매도세";
    }

    public String getTrendDescription() {
        switch (trendDirection) {
            case "RISING":
                return "상승 추세";
            case "FALLING":
                return "하락 추세";
            case "STABLE":
                return "안정";
            default:
                return "알 수 없음";
        }
    }

    @Override
    public String toString() {
        return String.format("TradeIntensity{code='%s', intensity=%.2f, sentiment='%s', trend='%s', alert='%s'}",
                code, intensityScore, marketSentiment, trendDirection, alertLevel);
    }
}
