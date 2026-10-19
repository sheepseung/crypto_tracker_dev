package com.crypto.flink.function;

import java.io.Serializable;
import java.util.List;

/**
 * 거래량 급증 기준선을 처음 채울 과거 구간별 거래대금(KRW)을 준다.
 * Flink 함수 안에 실리므로 직렬화 가능해야 하고, 연결 같은 자원은 transient 로 늦게 만든다.
 */
public interface BaselineSource extends Serializable {

    /** 오래된 것부터 최신 순으로, 최대 limit 개. 못 가져오면 빈 목록. */
    List<Double> recentBucketAmounts(String code, int bucketSeconds, int limit);

    static BaselineSource none() {
        return (code, bucketSeconds, limit) -> List.of();
    }
}
