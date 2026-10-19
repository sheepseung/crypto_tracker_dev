package com.crypto.flink.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** Kafka UI 와 대시보드에서 바로 읽히도록 한국 시간 ISO-8601 문자열을 만든다. */
public final class KstTime {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private KstTime() {
    }

    public static String format(long epochMillis) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(Instant.ofEpochMilli(epochMillis).atZone(KST));
    }
}
