package com.crypto.flink.summary;

import java.util.List;
import java.util.Map;

/** 하루(KST)치 요약. 금액은 KRW. coverage 는 그날 저장된 5분 버킷 비율(1.0 = 288개 전부). */
public record DailySummary(
        String date,
        long fromMillis,
        long toMillis,
        List<CodeSummary> codes,
        String topNetBuyCode,
        String topVolumeCode,
        int eventCount,
        Map<String, Integer> eventsByType,
        Map<String, Integer> eventsByCode,
        boolean complete) {

    public record CodeSummary(
            String code,
            double buyAmount,
            double sellAmount,
            double totalAmount,
            double netAmount,
            double intensity,
            int tradeCount,
            int buckets,
            double coverage) {
    }
}
