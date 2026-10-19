package com.crypto.flink.dto;

import com.crypto.flink.util.KstTime;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * F2·F3 감지 이벤트. type 으로 종류를 구분하고, 종류에 없는 필드는 null 로 두어 JSON 에서 빠진다.
 * 금액은 KRW. Flink POJO 요건(기본 생성자, getter/setter)을 지킨다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DetectionEvent {

    public static final String LARGE_TRADE = "LARGE_TRADE";
    public static final String LARGE_TRADE_BURST = "LARGE_TRADE_BURST";
    public static final String PRICE_SURGE = "PRICE_SURGE";
    public static final String PRICE_DROP = "PRICE_DROP";

    private String type;
    private String code;
    private long eventTime;
    private String eventTimeAt;
    private String side;
    private Double amount;
    private Double price;
    private Integer tradeCount;
    private Integer windowSeconds;
    private Double priceFrom;
    private Double priceTo;
    private Double changePercent;
    private String message;

    public DetectionEvent() {
    }

    public static DetectionEvent largeTrade(CryptoTrade trade) {
        DetectionEvent event = base(DetectionEvent.LARGE_TRADE, trade.getCode(), trade.getTradeTimestamp());
        event.side = trade.getAskBid();
        event.amount = trade.getTradeAmount();
        event.price = trade.getTradePrice();
        event.tradeCount = 1;
        event.message = String.format("%s %s %s 단일 체결 @ %s",
                trade.getCode(), sideLabel(trade.getAskBid()), Krw.format(event.amount), Krw.format(event.price));
        return event;
    }

    public static DetectionEvent largeTradeBurst(CryptoTrade lastTrade, double amount, int tradeCount,
            int windowSeconds) {
        DetectionEvent event = base(DetectionEvent.LARGE_TRADE_BURST, lastTrade.getCode(), lastTrade.getTradeTimestamp());
        event.side = lastTrade.getAskBid();
        event.amount = amount;
        event.price = lastTrade.getTradePrice();
        event.tradeCount = tradeCount;
        event.windowSeconds = windowSeconds;
        event.message = String.format("%s %s %s 가 %d초 안에 %d건으로 집중 체결",
                lastTrade.getCode(), sideLabel(lastTrade.getAskBid()), Krw.format(amount), windowSeconds, tradeCount);
        return event;
    }

    public static DetectionEvent priceMove(CryptoTrade trade, double priceFrom, double changePercent,
            int windowSeconds) {
        boolean up = changePercent > 0;
        DetectionEvent event = base(up ? PRICE_SURGE : PRICE_DROP, trade.getCode(), trade.getTradeTimestamp());
        event.price = trade.getTradePrice();
        event.priceFrom = priceFrom;
        event.priceTo = trade.getTradePrice();
        event.changePercent = changePercent;
        event.windowSeconds = windowSeconds;
        event.message = String.format("%s %d분 내 %s%.2f%% (%s → %s)",
                trade.getCode(), windowSeconds / 60, up ? "+" : "", changePercent,
                Krw.format(priceFrom), Krw.format(trade.getTradePrice()));
        return event;
    }

    private static DetectionEvent base(String type, String code, long eventTime) {
        DetectionEvent event = new DetectionEvent();
        event.type = type;
        event.code = code;
        event.eventTime = eventTime;
        event.eventTimeAt = KstTime.format(eventTime);
        return event;
    }

    private static String sideLabel(String askBid) {
        return CryptoTrade.BID.equals(askBid) ? "매수" : "매도";
    }

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

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventTimeAt() {
        return eventTimeAt;
    }

    public void setEventTimeAt(String eventTimeAt) {
        this.eventTimeAt = eventTimeAt;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Integer getTradeCount() {
        return tradeCount;
    }

    public void setTradeCount(Integer tradeCount) {
        this.tradeCount = tradeCount;
    }

    public Integer getWindowSeconds() {
        return windowSeconds;
    }

    public void setWindowSeconds(Integer windowSeconds) {
        this.windowSeconds = windowSeconds;
    }

    public Double getPriceFrom() {
        return priceFrom;
    }

    public void setPriceFrom(Double priceFrom) {
        this.priceFrom = priceFrom;
    }

    public Double getPriceTo() {
        return priceTo;
    }

    public void setPriceTo(Double priceTo) {
        this.priceTo = priceTo;
    }

    public Double getChangePercent() {
        return changePercent;
    }

    public void setChangePercent(Double changePercent) {
        this.changePercent = changePercent;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "DetectionEvent{" + type + " " + message + " at " + eventTimeAt + "}";
    }
}
