package com.crypto.flink.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

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
    // 체결 한 건 금액으로 나눈 주체 구간(개미…고래)별 매수·매도·건수. 오름차순이고 구간 경계는 설정(tiers.*).
    // 기본형 배열인 이유: List 필드는 Flink 가 Kryo 를 타는데 Java 17 에서는 JVM 옵션 없이 초기화에 실패한다
    private String[] tierNames = new String[0];
    private double[] tierMins = new double[0];
    private double[] tierBuyAmounts = new double[0];
    private double[] tierSellAmounts = new double[0];
    private int[] tierTradeCounts = new int[0];
    private double openPrice;
    private double closePrice;

    public TradeIntensity() {
    }

    /** 윈도우 안 가격 변화(%). 시가가 없으면 0. */
    public double getPriceChangePercent() {
        return openPrice > 0 ? (closePrice / openPrice - 1.0) * 100.0 : 0.0;
    }

    /** 구간 목록 뷰. JSON 에는 읽기 전용으로 실리고, 소비자는 배열 필드로 역직렬화한다. */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public List<TierAmount> getTiers() {
        List<TierAmount> list = new ArrayList<>(tierNames.length);
        for (int i = 0; i < tierNames.length; i++) {
            TierAmount t = new TierAmount(tierNames[i], tierMins[i]);
            t.buyAmount = tierBuyAmounts[i];
            t.sellAmount = tierSellAmounts[i];
            t.tradeCount = tierTradeCounts[i];
            list.add(t);
        }
        return list;
    }

    /** 테스트와 수동 구성용. 배열 필드를 채운다. */
    @JsonIgnore
    public void setTiers(List<TierAmount> tiers) {
        int n = tiers.size();
        tierNames = new String[n];
        tierMins = new double[n];
        tierBuyAmounts = new double[n];
        tierSellAmounts = new double[n];
        tierTradeCounts = new int[n];
        for (int i = 0; i < n; i++) {
            TierAmount t = tiers.get(i);
            tierNames[i] = t.name;
            tierMins[i] = t.minAmount;
            tierBuyAmounts[i] = t.buyAmount;
            tierSellAmounts[i] = t.sellAmount;
            tierTradeCounts[i] = t.tradeCount;
        }
    }

    /** 가장 큰 구간(고래). 구간이 없으면 빈 값. */
    @JsonIgnore
    public TierAmount whaleTier() {
        return tierNames.length == 0 ? new TierAmount() : getTiers().get(tierNames.length - 1);
    }

    /** 가장 작은 구간(개미). */
    @JsonIgnore
    public TierAmount retailTier() {
        return tierNames.length == 0 ? new TierAmount() : getTiers().get(0);
    }

    public double getWhaleNetAmount() {
        return whaleTier().net();
    }

    public double getRetailNetAmount() {
        return retailTier().net();
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

    public String[] getTierNames() {
        return tierNames;
    }

    public void setTierNames(String[] tierNames) {
        this.tierNames = tierNames;
    }

    public double[] getTierMins() {
        return tierMins;
    }

    public void setTierMins(double[] tierMins) {
        this.tierMins = tierMins;
    }

    public double[] getTierBuyAmounts() {
        return tierBuyAmounts;
    }

    public void setTierBuyAmounts(double[] tierBuyAmounts) {
        this.tierBuyAmounts = tierBuyAmounts;
    }

    public double[] getTierSellAmounts() {
        return tierSellAmounts;
    }

    public void setTierSellAmounts(double[] tierSellAmounts) {
        this.tierSellAmounts = tierSellAmounts;
    }

    public int[] getTierTradeCounts() {
        return tierTradeCounts;
    }

    public void setTierTradeCounts(int[] tierTradeCounts) {
        this.tierTradeCounts = tierTradeCounts;
    }

    public double getOpenPrice() {
        return openPrice;
    }

    public void setOpenPrice(double openPrice) {
        this.openPrice = openPrice;
    }

    public double getClosePrice() {
        return closePrice;
    }

    public void setClosePrice(double closePrice) {
        this.closePrice = closePrice;
    }

    @Override
    public String toString() {
        return String.format("TradeIntensity{code='%s', window=%ds, intensity=%.2f, net=%.0f, count=%d, end=%s}",
                code, windowSeconds, intensity, netAmount, tradeCount, windowEndAt);
    }
}
