package com.crypto.flink.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 업비트 WebSocket trade 메시지 한 건.
 * Flink POJO 직렬화 요건(public 기본 생성자, 모든 필드의 getter/setter)을 지킨다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CryptoTrade {

    public static final String TYPE_TRADE = "trade";
    public static final String ASK = "ASK";
    public static final String BID = "BID";

    @JsonProperty("type")
    private String type;

    @JsonProperty("code")
    private String code;

    @JsonProperty("trade_price")
    private Double tradePrice;

    @JsonProperty("trade_volume")
    private Double tradeVolume;

    // 업비트 기준 BID 는 매수 주문이 체결된 건, ASK 는 매도 주문이 체결된 건
    @JsonProperty("ask_bid")
    private String askBid;

    // 체결 시각(ms). timestamp 는 업비트가 메시지를 보낸 시각이라 이벤트 시간으로는 이 값을 쓴다
    @JsonProperty("trade_timestamp")
    private Long tradeTimestamp;

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("sequential_id")
    private Long sequentialId;

    @JsonProperty("prev_closing_price")
    private Double prevClosingPrice;

    @JsonProperty("change")
    private String change;

    @JsonProperty("change_price")
    private Double changePrice;

    @JsonProperty("stream_type")
    private String streamType;

    public CryptoTrade() {
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

    public String getAskBid() {
        return askBid;
    }

    public void setAskBid(String askBid) {
        this.askBid = askBid;
    }

    public Long getTradeTimestamp() {
        return tradeTimestamp;
    }

    public void setTradeTimestamp(Long tradeTimestamp) {
        this.tradeTimestamp = tradeTimestamp;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getSequentialId() {
        return sequentialId;
    }

    public void setSequentialId(Long sequentialId) {
        this.sequentialId = sequentialId;
    }

    public Double getPrevClosingPrice() {
        return prevClosingPrice;
    }

    public void setPrevClosingPrice(Double prevClosingPrice) {
        this.prevClosingPrice = prevClosingPrice;
    }

    public String getChange() {
        return change;
    }

    public void setChange(String change) {
        this.change = change;
    }

    public Double getChangePrice() {
        return changePrice;
    }

    public void setChangePrice(Double changePrice) {
        this.changePrice = changePrice;
    }

    public String getStreamType() {
        return streamType;
    }

    public void setStreamType(String streamType) {
        this.streamType = streamType;
    }

    @JsonIgnore
    public boolean isBid() {
        return BID.equals(askBid);
    }

    /** 체결 금액(KRW). 종목마다 수량 단위가 달라 비교와 임계치는 금액으로 한다. */
    @JsonIgnore
    public double getTradeAmount() {
        return tradePrice * tradeVolume;
    }

    @Override
    public String toString() {
        return "CryptoTrade{" +
                "code='" + code + '\'' +
                ", askBid='" + askBid + '\'' +
                ", tradePrice=" + tradePrice +
                ", tradeVolume=" + tradeVolume +
                ", tradeTimestamp=" + tradeTimestamp +
                ", sequentialId=" + sequentialId +
                '}';
    }
}
