package com.crypto.flink.dto;

/** 원화 금액을 사람이 읽는 단위로 만든다. 이벤트 메시지에만 쓴다. */
public final class Krw {

    private static final double EOK = 100_000_000;
    private static final double MAN = 10_000;

    private Krw() {
    }

    public static String format(double amount) {
        double abs = Math.abs(amount);
        if (abs >= EOK) {
            return String.format("%.2f억 원", amount / EOK);
        }
        if (abs >= MAN) {
            return String.format("%,.0f만 원", amount / MAN);
        }
        return String.format("%,.0f원", amount);
    }
}
