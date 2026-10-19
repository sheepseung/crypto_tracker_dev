package com.crypto.flink.orderbook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** 업비트 WebSocket orderbook 메시지. 수량은 코인 단위, 가격은 KRW. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Orderbook {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Unit {
        @JsonProperty("ask_price")
        public double askPrice;
        @JsonProperty("bid_price")
        public double bidPrice;
        @JsonProperty("ask_size")
        public double askSize;
        @JsonProperty("bid_size")
        public double bidSize;
    }

    @JsonProperty("type")
    public String type;
    @JsonProperty("code")
    public String code;
    @JsonProperty("timestamp")
    public long timestamp;
    @JsonProperty("total_ask_size")
    public double totalAskSize;
    @JsonProperty("total_bid_size")
    public double totalBidSize;
    // 업비트가 최우선 호가부터 순서대로 준다
    @JsonProperty("orderbook_units")
    public List<Unit> units = List.of();
}
