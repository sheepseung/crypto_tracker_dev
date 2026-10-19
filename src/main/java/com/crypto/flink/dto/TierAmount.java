package com.crypto.flink.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * 체결 한 건 금액 구간(주체) 하나의 매수·매도 누적. Flink 상태와 JSON 양쪽에 실리므로 public 필드 POJO 로 둔다.
 * name 과 minAmount 는 설정(tiers.*)에서 오며, 구간은 minAmount 이상 다음 구간 minAmount 미만이다.
 */
public class TierAmount {

    public String name;
    public double minAmount;
    public double buyAmount;
    public double sellAmount;
    public int tradeCount;

    public TierAmount() {
    }

    public TierAmount(String name, double minAmount) {
        this.name = name;
        this.minAmount = minAmount;
    }

    public TierAmount copy() {
        TierAmount t = new TierAmount(name, minAmount);
        t.buyAmount = buyAmount;
        t.sellAmount = sellAmount;
        t.tradeCount = tradeCount;
        return t;
    }

    @JsonIgnore
    public double net() {
        return buyAmount - sellAmount;
    }

    @JsonIgnore
    public double total() {
        return buyAmount + sellAmount;
    }
}
