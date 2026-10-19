package com.crypto.flink.controller;

import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.store.MetricsStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/intensity")
@Tag(name = "Trade Intensity", description = "종목별 체결강도·순매수 지표 (Flink 집계 결과의 최신 값)")
public class IntensityController {

    private static final Map<String, Comparator<TradeIntensity>> SORTS = Map.of(
            "netAmount", Comparator.comparingDouble(TradeIntensity::getNetAmount).reversed(),
            "intensity", Comparator.comparingDouble(TradeIntensity::getIntensity).reversed(),
            "totalAmount", Comparator.comparingDouble(TradeIntensity::getTotalAmount).reversed(),
            "code", Comparator.comparing(TradeIntensity::getCode));

    private final MetricsStore metricsStore;

    public IntensityController(MetricsStore metricsStore) {
        this.metricsStore = metricsStore;
    }

    @GetMapping
    @Operation(summary = "구간별 전 종목 체결강도", description = "window 는 초 단위 집계 구간(60, 300, 900). sort 는 netAmount, intensity, totalAmount, code")
    public List<TradeIntensity> byWindow(
            @RequestParam(defaultValue = "300") int window,
            @RequestParam(defaultValue = "netAmount") String sort) {
        Comparator<TradeIntensity> comparator = SORTS.get(sort);
        if (comparator == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "sort 는 " + SORTS.keySet() + " 중 하나");
        }
        return metricsStore.byWindow(window).stream().sorted(comparator).toList();
    }

    @GetMapping("/{code}")
    @Operation(summary = "한 종목의 모든 구간 체결강도", description = "구간(초)을 키로 하는 맵")
    public Map<Integer, TradeIntensity> byCode(@PathVariable String code) {
        return metricsStore.byCode(code.toUpperCase())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "아직 지표가 없는 종목: " + code));
    }
}
