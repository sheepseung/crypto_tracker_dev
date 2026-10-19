package com.crypto.flink.store;

import com.crypto.flink.dto.TradeIntensity;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 종목·구간별 최신 체결강도만 메모리에 둔다.
 * 지표는 5초마다 새로 오고 과거 값은 쓸 데가 없어 DB 에 넣지 않는다.
 */
@Component
public class MetricsStore {

    private final Map<String, Map<Integer, TradeIntensity>> latestByCode = new ConcurrentHashMap<>();

    public void put(TradeIntensity intensity) {
        latestByCode
                .computeIfAbsent(intensity.getCode(), code -> new ConcurrentHashMap<>())
                // Kafka 재소비나 지연 도착으로 옛 윈도우가 뒤늦게 와도 최신 값을 덮어쓰지 않는다
                .merge(intensity.getWindowSeconds(), intensity,
                        (current, incoming) -> incoming.getWindowEnd() >= current.getWindowEnd() ? incoming : current);
    }

    /** 한 구간의 모든 종목 값. 정렬은 호출하는 쪽이 정한다. */
    public List<TradeIntensity> byWindow(int windowSeconds) {
        return latestByCode.values().stream()
                .map(windows -> windows.get(windowSeconds))
                .filter(java.util.Objects::nonNull)
                .sorted(Comparator.comparing(TradeIntensity::getCode))
                .toList();
    }

    /** 한 종목의 모든 구간 값. 구간 오름차순. */
    public Optional<Map<Integer, TradeIntensity>> byCode(String code) {
        Map<Integer, TradeIntensity> windows = latestByCode.get(code);
        if (windows == null) {
            return Optional.empty();
        }
        return Optional.of(Collections.unmodifiableMap(new java.util.TreeMap<>(windows)));
    }

    public Collection<String> codes() {
        return latestByCode.keySet().stream().sorted().toList();
    }

    /** 모든 종목·구간의 최신 값. 대시보드 첫 화면용. */
    public List<TradeIntensity> all() {
        return latestByCode.values().stream()
                .flatMap(windows -> windows.values().stream())
                .sorted(Comparator.comparing(TradeIntensity::getCode).thenComparingInt(TradeIntensity::getWindowSeconds))
                .toList();
    }
}
