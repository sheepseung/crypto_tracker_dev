package com.crypto.flink.controller;

import com.crypto.flink.store.IntensityBucketRepository;
import com.crypto.flink.store.IntensityBucketRepository.Bucket;
import com.crypto.flink.summary.DailySummaryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 그래프용 이력. 저장된 5분 버킷으로 만든다. 업비트가 보여 주는 현재 값이 아니라 흐름을 시간축으로 보여 주는 것이 목적이다.
 */
@RestController
@RequestMapping("/api/history")
@Tag(name = "History", description = "5분 버킷 이력. 종목별 매수 비율·순매수 추이와 하루 누적 순매수")
public class HistoryController {

    private static final int MAX_HOURS = 72;

    private final IntensityBucketRepository buckets;

    public HistoryController(IntensityBucketRepository buckets) {
        this.buckets = buckets;
    }

    public record Point(long t, double buyAmount, double sellAmount, double netAmount, double totalAmount,
            double buyRatio, double intensity, int tradeCount, double whaleNet, double midNet, double retailNet) {
        static Point of(Bucket b) {
            return new Point(b.windowEnd(), b.buyAmount(), b.sellAmount(), b.netAmount(), b.totalAmount(),
                    b.buyRatio(), b.intensity(), b.tradeCount(), b.whaleNet(), b.midNet(), b.retailNet());
        }
    }

    public record CumulativePoint(long t, double cumulativeNet, double cumulativeTotal,
            double cumulativeWhaleNet, double cumulativeMidNet, double cumulativeRetailNet) {
    }

    @GetMapping
    @Operation(summary = "최근 N시간 5분 버킷", description = "종목별 목록. code 를 주면 그 종목만")
    public Map<String, List<Point>> recent(
            @RequestParam(required = false) String code,
            @RequestParam(defaultValue = "6") int hours) {
        if (hours < 1 || hours > MAX_HOURS) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "hours 는 1~" + MAX_HOURS);
        }
        long to = System.currentTimeMillis();
        long from = to - hours * 3600_000L;
        Map<String, List<Point>> byCode = new LinkedHashMap<>();
        for (Bucket b : buckets.bucketsBetween(from, to)) {
            if (code != null && !code.equalsIgnoreCase(b.code())) {
                continue;
            }
            byCode.computeIfAbsent(b.code(), c -> new ArrayList<>()).add(Point.of(b));
        }
        return byCode;
    }

    @GetMapping("/cumulative")
    @Operation(summary = "하루(KST) 누적 순매수·누적 거래대금", description = "date 는 YYYY-MM-DD, 비우면 오늘. 5분마다 한 점")
    public Map<String, List<CumulativePoint>> cumulative(@RequestParam(required = false) String date) {
        LocalDate day = date == null ? LocalDate.now(DailySummaryService.KST) : parse(date);
        long from = day.atStartOfDay(DailySummaryService.KST).toInstant().toEpochMilli();
        long to = day.plusDays(1).atStartOfDay(DailySummaryService.KST).toInstant().toEpochMilli();
        Map<String, List<CumulativePoint>> byCode = new LinkedHashMap<>();
        Map<String, double[]> running = new LinkedHashMap<>();
        for (Bucket b : buckets.bucketsBetween(from, to)) {
            double[] sum = running.computeIfAbsent(b.code(), c -> new double[5]);
            sum[0] += b.netAmount();
            sum[1] += b.totalAmount();
            sum[2] += b.whaleNet();
            sum[3] += b.midNet();
            sum[4] += b.retailNet();
            byCode.computeIfAbsent(b.code(), c -> new ArrayList<>())
                    .add(new CumulativePoint(b.windowEnd(), sum[0], sum[1], sum[2], sum[3], sum[4]));
        }
        return byCode;
    }

    private static LocalDate parse(String date) {
        try {
            return LocalDate.parse(date);
        } catch (java.time.format.DateTimeParseException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "date 는 YYYY-MM-DD: " + date);
        }
    }
}
