package com.crypto.flink.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

/**
 * 암호화폐 거래 데이터 DTO
 * 업비트 API에서 수신하는 실시간 거래 정보를 담습니다.
 */
public class CryptoTrade {

    @JsonProperty("type")
    private String type;

    @JsonProperty("code")
    private String code;

    @JsonProperty("trade_price")
    private Double tradePrice;

    @JsonProperty("trade_volume")
    private Double tradeVolume;

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("change")
    private String change;

    @JsonProperty("change_rate")
    private Double changeRate;

    @JsonProperty("acc_trade_price")
    private Double accTradePrice;

    @JsonProperty("acc_trade_volume")
    private Double accTradeVolume;

    // 생성자
    public CryptoTrade() {
    }

    public CryptoTrade(String type, String code, Double tradePrice, Double tradeVolume,
            Long timestamp, String change, Double changeRate,
            Double accTradePrice, Double accTradeVolume) {
        this.type = type;
        this.code = code;
        this.tradePrice = tradePrice;
        this.tradeVolume = tradeVolume;
        this.timestamp = timestamp;
        this.change = change;
        this.changeRate = changeRate;
        this.accTradePrice = accTradePrice;
        this.accTradeVolume = accTradeVolume;
    }

    // Getter & Setter
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public Double getTradeVolume() {
        return tradeVolume;
    }

    public void setTradeVolume(Double tradeVolume) {
        this.tradeVolume = tradeVolume;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getChange() {
        return change;
    }

    public void setChange(String change) {
        this.change = change;
    }

    public Double getChangeRate() {
        return changeRate;
    }

    public void setChangeRate(Double changeRate) {
        this.changeRate = changeRate;
    }

    public Double getAccTradePrice() {
        return accTradePrice;
    }

    public void setAccTradePrice(Double accTradePrice) {
        this.accTradePrice = accTradePrice;
    }

    public Double getAccTradeVolume() {
        return accTradeVolume;
    }

    public void setAccTradeVolume(Double accTradeVolume) {
        this.accTradeVolume = accTradeVolume;
    }

    /**
     * 타임스탬프를 LocalDateTime으로 변환
     */
    public LocalDateTime getTradeDateTime() {
        if (timestamp != null) {
            return LocalDateTime.ofEpochSecond(timestamp / 1000, 0, java.time.ZoneOffset.UTC);
        }
        return null;
    }

    /**
     * 가격 변동률을 퍼센트로 변환
     */
    public Double getChangeRatePercent() {
        if (changeRate != null) {
            return changeRate * 100;
        }
        return 0.0;
    }

    @Override
    public String toString() {
        return "CryptoTrade{" +
                "type='" + type + '\'' +
                ", code='" + code + '\'' +
                ", tradePrice=" + tradePrice +
                ", tradeVolume=" + tradeVolume +
                ", timestamp=" + timestamp +
                ", change='" + change + '\'' +
                ", changeRate=" + changeRate +
                ", accTradePrice=" + accTradePrice +
                ", accTradeVolume=" + accTradeVolume +
                '}';
    }
}
