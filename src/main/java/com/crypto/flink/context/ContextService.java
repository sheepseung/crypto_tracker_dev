package com.crypto.flink.context;

import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.store.IntensityBucketRepository;
import com.crypto.flink.store.IntensityBucketRepository.Bucket;
import com.crypto.flink.store.MetricsStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * G4 평소 대비 백분위. 종목별 최근 N일 5분 버킷 분포에서 지금 5분 값이 어디쯤인지 돌려준다.
 * 절대값만 주는 업비트와 달리 "지금이 특별한가"를 숫자 하나로 말한다. 분포는 5분마다 다시 읽는다.
 */
@Service
public class ContextService {

    public record CodeContext(String code, Double intensityPercentile, Double totalAmountPercentile,
            Double netAmountPercentile, int samples, int days) {
    }

    private record Distribution(double[] intensity, double[] totalAmount, double[] netAmount) {
    }

    private final IntensityBucketRepository buckets;
    private final MetricsStore metricsStore;
    private final int days;
    private volatile Map<String, Distribution> distributions = Map.of();

    public ContextService(IntensityBucketRepository buckets, MetricsStore metricsStore,
            @Value("${context.days:7}") int days) {
        this.buckets = buckets;
        this.metricsStore = metricsStore;
        this.days = days;
    }

    @Scheduled(initialDelay = 10_000, fixedDelayString = "${context.refresh-seconds:300}000")
    public void refresh() {
        long to = System.currentTimeMillis();
        long from = to - days * 86_400_000L;
        Map<String, List<Bucket>> byCode = new HashMap<>();
        for (Bucket b : buckets.bucketsBetween(from, to)) {
            byCode.computeIfAbsent(b.code(), c -> new java.util.ArrayList<>()).add(b);
        }
        Map<String, Distribution> next = new HashMap<>();
        byCode.forEach((code, list) -> next.put(code, new Distribution(
                sorted(list, Bucket::intensity), sorted(list, Bucket::totalAmount), sorted(list, Bucket::netAmount))));
        distributions = next;
    }

    public List<CodeContext> current() {
        return metricsStore.codes().stream()
                .map(code -> metricsStore.byCode(code).map(w -> w.get(IntensityBucketRepository.BUCKET_SECONDS)))
                .flatMap(Optional::stream)
                .map(this::contextOf)
                .toList();
    }

    /** 다른 감지기가 쓰는 백분위. 분포가 없으면 비어 있다. */
    public Optional<CodeContext> contextFor(String code, double intensity, double totalAmount, double netAmount) {
        Distribution d = distributions.get(code);
        if (d == null || d.intensity().length == 0) {
            return Optional.empty();
        }
        return Optional.of(new CodeContext(code, percentile(d.intensity(), intensity),
                percentile(d.totalAmount(), totalAmount), percentile(d.netAmount(), netAmount),
                d.intensity().length, days));
    }

    private CodeContext contextOf(TradeIntensity w) {
        Distribution d = distributions.get(w.getCode());
        if (d == null || d.intensity().length == 0) {
            return new CodeContext(w.getCode(), null, null, null, 0, days);
        }
        return new CodeContext(w.getCode(),
                percentile(d.intensity(), w.getIntensity()),
                percentile(d.totalAmount(), w.getTotalAmount()),
                percentile(d.netAmount(), w.getNetAmount()),
                d.intensity().length, days);
    }

    /** 정렬된 표본에서 value 이하인 비율(0~100). 100 에 가까울수록 평소보다 높다. */
    static double percentile(double[] sortedSamples, double value) {
        int idx = Arrays.binarySearch(sortedSamples, value);
        int countAtOrBelow = idx >= 0 ? idx + 1 : -idx - 1;
        // 같은 값이 여럿이면 그 뒤까지 포함한다
        while (idx >= 0 && countAtOrBelow < sortedSamples.length && sortedSamples[countAtOrBelow] == value) {
            countAtOrBelow++;
        }
        return 100.0 * countAtOrBelow / sortedSamples.length;
    }

    private static double[] sorted(List<Bucket> list, java.util.function.ToDoubleFunction<Bucket> f) {
        double[] values = list.stream().mapToDouble(f).toArray();
        Arrays.sort(values);
        return values;
    }
}
