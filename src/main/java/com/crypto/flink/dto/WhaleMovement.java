package com.crypto.flink.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

/**
 * 고래(대량 거래자) 움직임을 감지하기 위한 DTO
 * 
 * 주요 정보:
 * - 특정 시간 윈도우 내의 매수/매도 체결량 집계
 * - 이상 거래량 감지 결과
 * - 고래 움직임 패턴 분석
 */
public class WhaleMovement {

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("code")
    private String code;

    @JsonProperty("window_start")
    private String windowStart;

    @JsonProperty("window_end")
    private String windowEnd;

    @JsonProperty("window_duration_seconds")
    private Integer windowDurationSeconds;

    @JsonProperty("total_buy_volume")
    private Double totalBuyVolume;

    @JsonProperty("total_sell_volume")
    private Double totalSellVolume;

    @JsonProperty("net_volume")
    private Double netVolume;

    @JsonProperty("volume_ratio")
    private Double volumeRatio;

    @JsonProperty("whale_threshold")
    private Double whaleThreshold;

    @JsonProperty("is_whale_movement")
    private Boolean isWhaleMovement;

    @JsonProperty("movement_type")
    private String movementType; // "AGGRESSIVE_BUY", "AGGRESSIVE_SELL", "BALANCED", "NORMAL"

    @JsonProperty("confidence_score")
    private Double confidenceScore;

    @JsonProperty("pattern_detected")
    private String patternDetected;

    @JsonProperty("alert_level")
    private String alertLevel; // "LOW", "MEDIUM", "HIGH", "CRITICAL"

    @JsonProperty("processed_at")
    private LocalDateTime processedAt;

    // 생성자
    public WhaleMovement() {
        this.processedAt = LocalDateTime.now();
    }

    public WhaleMovement(String timestamp, String code, String windowStart, String windowEnd,
            Integer windowDurationSeconds, Double totalBuyVolume, Double totalSellVolume,
            Double netVolume, Double volumeRatio, Double whaleThreshold,
            Boolean isWhaleMovement, String movementType, Double confidenceScore,
            String patternDetected, String alertLevel) {
        this.timestamp = timestamp;
        this.code = code;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.windowDurationSeconds = windowDurationSeconds;
        this.totalBuyVolume = totalBuyVolume;
        this.totalSellVolume = totalSellVolume;
        this.netVolume = netVolume;
        this.volumeRatio = volumeRatio;
        this.whaleThreshold = whaleThreshold;
        this.isWhaleMovement = isWhaleMovement;
        this.movementType = movementType;
        this.confidenceScore = confidenceScore;
        this.patternDetected = patternDetected;
        this.alertLevel = alertLevel;
        this.processedAt = LocalDateTime.now();
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

    public Integer getWindowDurationSeconds() {
        return windowDurationSeconds;
    }

    public void setWindowDurationSeconds(Integer windowDurationSeconds) {
        this.windowDurationSeconds = windowDurationSeconds;
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

    public Double getNetVolume() {
        return netVolume;
    }

    public void setNetVolume(Double netVolume) {
        this.netVolume = netVolume;
    }

    public Double getVolumeRatio() {
        return volumeRatio;
    }

    public void setVolumeRatio(Double volumeRatio) {
        this.volumeRatio = volumeRatio;
    }

    public Double getWhaleThreshold() {
        return whaleThreshold;
    }

    public void setWhaleThreshold(Double whaleThreshold) {
        this.whaleThreshold = whaleThreshold;
    }

    public Boolean getIsWhaleMovement() {
        return isWhaleMovement;
    }

    public void setIsWhaleMovement(Boolean isWhaleMovement) {
        this.isWhaleMovement = isWhaleMovement;
    }

    public String getMovementType() {
        return movementType;
    }

    public void setMovementType(String movementType) {
        this.movementType = movementType;
    }

    public Double getConfidenceScore() {
        return confidenceScore;
    }

    public void setConfidenceScore(Double confidenceScore) {
        this.confidenceScore = confidenceScore;
    }

    public String getPatternDetected() {
        return patternDetected;
    }

    public void setPatternDetected(String patternDetected) {
        this.patternDetected = patternDetected;
    }

    public String getAlertLevel() {
        return alertLevel;
    }

    public void setAlertLevel(String alertLevel) {
        this.alertLevel = alertLevel;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }

    /**
     * 고래 움직임 유형에 따른 색상 반환 (UI 표시용)
     */
    public String getMovementColor() {
        switch (movementType) {
            case "AGGRESSIVE_BUY":
                return "success";
            case "AGGRESSIVE_SELL":
                return "danger";
            case "BALANCED":
                return "warning";
            case "NORMAL":
                return "info";
            default:
                return "secondary";
        }
    }

    /**
     * 알림 레벨에 따른 아이콘 반환
     */
    public String getAlertIcon() {
        switch (alertLevel) {
            case "CRITICAL":
                return "🚨";
            case "HIGH":
                return "⚠️";
            case "MEDIUM":
                return "📊";
            case "LOW":
                return "📈";
            default:
                return "ℹ️";
        }
    }

    /**
     * 고래 움직임 설명 생성
     */
    public String getMovementDescription() {
        if (!isWhaleMovement) {
            return "정상적인 거래량 범위";
        }

        StringBuilder description = new StringBuilder();

        if (movementType.equals("AGGRESSIVE_BUY")) {
            description.append("대량 매수 세력 진입 감지");
        } else if (movementType.equals("AGGRESSIVE_SELL")) {
            description.append("대량 매도 세력 이탈 감지");
        } else if (movementType.equals("BALANCED")) {
            description.append("대량 거래 세력 균형 유지");
        }

        description.append(" (순거래량: ").append(String.format("%.4f", netVolume)).append(")");

        return description.toString();
    }

    @Override
    public String toString() {
        return "WhaleMovement{" +
                "timestamp='" + timestamp + '\'' +
                ", code='" + code + '\'' +
                ", windowStart='" + windowStart + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", windowDurationSeconds=" + windowDurationSeconds +
                ", totalBuyVolume=" + totalBuyVolume +
                ", totalSellVolume=" + totalSellVolume +
                ", netVolume=" + netVolume +
                ", volumeRatio=" + volumeRatio +
                ", whaleThreshold=" + whaleThreshold +
                ", isWhaleMovement=" + isWhaleMovement +
                ", movementType='" + movementType + '\'' +
                ", confidenceScore=" + confidenceScore +
                ", patternDetected='" + patternDetected + '\'' +
                ", alertLevel='" + alertLevel + '\'' +
                ", processedAt=" + processedAt +
                '}';
    }
}
