package com.crypto.flink.dto;

/**
 * 한 종목의 한 집계 구간에 대한 체결강도 지표.
 * 금액은 모두 KRW. Flink POJO 요건(기본 생성자, getter/setter)을 지킨다.
 */
public class TradeIntensity {

    // 매도 체결이 없으면 체결강도가 무한대라 JSON 으로 못 내보내므로 이 값으로 막는다
    public static final double MAX_INTENSITY = 9999.99;

    private String code;
    private int windowSeconds;
    private long windowStart;
    private long windowEnd;
    private String windowEndAt;
    private double buyAmount;
    private double sellAmount;
    private double totalAmount;
    private double netAmount;
    private double buyVolume;
    private double sellVolume;
    private int tradeCount;
    private double intensity;
    private double buyRatio;
    private long lastTradeTimestamp;

    public TradeIntensity() {
    }

    public static double intensityOf(double buyAmount, double sellAmount) {
        if (sellAmount <= 0) {
            return buyAmount <= 0 ? 100.0 : MAX_INTENSITY;
        }
        return Math.min(buyAmount / sellAmount * 100.0, MAX_INTENSITY);
    }

    public static double buyRatioOf(double buyAmount, double sellAmount) {
        double total = buyAmount + sellAmount;
        return total <= 0 ? 50.0 : buyAmount / total * 100.0;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public int getWindowSeconds() {
        return windowSeconds;
    }

    public void setWindowSeconds(int windowSeconds) {
        this.windowSeconds = windowSeconds;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getWindowEndAt() {
        return windowEndAt;
    }

    public void setWindowEndAt(String windowEndAt) {
        this.windowEndAt = windowEndAt;
    }

    public double getBuyAmount() {
        return buyAmount;
    }

    public void setBuyAmount(double buyAmount) {
        this.buyAmount = buyAmount;
    }

    public double getSellAmount() {
        return sellAmount;
    }

    public void setSellAmount(double sellAmount) {
        this.sellAmount = sellAmount;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public double getNetAmount() {
        return netAmount;
    }

    public void setNetAmount(double netAmount) {
        this.netAmount = netAmount;
    }

    public double getBuyVolume() {
        return buyVolume;
    }

    public void setBuyVolume(double buyVolume) {
        this.buyVolume = buyVolume;
    }

    public double getSellVolume() {
        return sellVolume;
    }

    public void setSellVolume(double sellVolume) {
        this.sellVolume = sellVolume;
    }

    public int getTradeCount() {
        return tradeCount;
    }

    public void setTradeCount(int tradeCount) {
        this.tradeCount = tradeCount;
    }

    public double getIntensity() {
        return intensity;
    }

    public void setIntensity(double intensity) {
        this.intensity = intensity;
    }

    public double getBuyRatio() {
        return buyRatio;
    }

    public void setBuyRatio(double buyRatio) {
        this.buyRatio = buyRatio;
    }

    public long getLastTradeTimestamp() {
        return lastTradeTimestamp;
    }

    public void setLastTradeTimestamp(long lastTradeTimestamp) {
        this.lastTradeTimestamp = lastTradeTimestamp;
    }

    @Override
    public String toString() {
        return String.format("TradeIntensity{code='%s', window=%ds, intensity=%.2f, net=%.0f, count=%d, end=%s}",
                code, windowSeconds, intensity, netAmount, tradeCount, windowEndAt);
    }
}
