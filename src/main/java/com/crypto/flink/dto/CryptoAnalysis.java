package com.crypto.flink.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

/**
 * 암호화폐 분석 결과 DTO
 * Flink 스트리밍 처리 결과를 담습니다.
 */
public class CryptoAnalysis {

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("code")
    private String code;

    @JsonProperty("trade_price")
    private Double tradePrice;

    @JsonProperty("price_change")
    private Double priceChange;

    @JsonProperty("trend")
    private String trend;

    @JsonProperty("analysis")
    private String analysis;

    @JsonProperty("volatility_score")
    private Double volatilityScore;

    @JsonProperty("processed_at")
    private LocalDateTime processedAt;

    // 생성자
    public CryptoAnalysis() {
        this.processedAt = LocalDateTime.now();
    }

    public CryptoAnalysis(String timestamp, String code, Double tradePrice,
            Double priceChange, String trend, String analysis,
            Double volatilityScore) {
        this.timestamp = timestamp;
        this.code = code;
        this.tradePrice = tradePrice;
        this.priceChange = priceChange;
        this.trend = trend;
        this.analysis = analysis;
        this.volatilityScore = volatilityScore;
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

    public Double getTradePrice() {
        return tradePrice;
    }

    public void setTradePrice(Double tradePrice) {
        this.tradePrice = tradePrice;
    }

    public Double getPriceChange() {
        return priceChange;
    }

    public void setPriceChange(Double priceChange) {
        this.priceChange = priceChange;
    }

    public String getTrend() {
        return trend;
    }

    public void setTrend(String trend) {
        this.trend = trend;
    }

    public String getAnalysis() {
        return analysis;
    }

    public void setAnalysis(String analysis) {
        this.analysis = analysis;
    }

    public Double getVolatilityScore() {
        return volatilityScore;
    }

    public void setVolatilityScore(Double volatilityScore) {
        this.volatilityScore = volatilityScore;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }

    /**
     * 트렌드에 따른 색상 반환 (UI 표시용)
     */
    public String getTrendColor() {
        switch (trend) {
            case "STRONG_UP":
                return "success";
            case "UP":
                return "info";
            case "STABLE":
                return "warning";
            case "DOWN":
                return "warning";
            case "STRONG_DOWN":
                return "danger";
            default:
                return "secondary";
        }
    }

    /**
     * 변동성 수준 반환
     */
    public String getVolatilityLevel() {
        if (volatilityScore == null)
            return "UNKNOWN";

        if (volatilityScore > 10.0)
            return "HIGH";
        else if (volatilityScore > 5.0)
            return "MEDIUM";
        else
            return "LOW";
    }

    @Override
    public String toString() {
        return "CryptoAnalysis{" +
                "timestamp='" + timestamp + '\'' +
                ", code='" + code + '\'' +
                ", tradePrice=" + tradePrice +
                ", priceChange=" + priceChange +
                ", trend='" + trend + '\'' +
                ", analysis='" + analysis + '\'' +
                ", volatilityScore=" + volatilityScore +
                ", processedAt=" + processedAt +
                '}';
    }
}
