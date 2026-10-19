package com.crypto.flink.function;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.rules.DetectionRule;
import com.crypto.flink.rules.DetectionRules;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * F5 거래량 급증 감지. 슬라이딩 구간 거래대금이 과거 같은 길이 구간들의 평균(기준선)의 multiplier 배 이상이면 낸다.
 * 기준선은 구간 길이에 맞춰 떨어지는 윈도우(정렬 버킷)의 거래대금을 최근 baselineBuckets 개 모아 평균 낸다.
 * 처음에는 BaselineSource 로 과거 버킷을 채우고, 이후 스트림으로 갱신한다. 시간은 체결 시각 기준이다.
 * 배수와 cooldown 은 종목별 규칙에서 읽는다.
 */
public class VolumeSurgeDetector extends KeyedProcessFunction<String, TradeIntensity, DetectionEvent> {

    private final int bucketSeconds;
    private final int baselineBuckets;
    private final int minBaselineBuckets;
    private final DetectionRules rules;
    private final BaselineSource baselineSource;

    private transient ListState<Double> buckets;
    private transient ValueState<Long> lastBucketEnd;
    private transient ValueState<Boolean> seeded;
    private transient ValueState<Long> lastEmitted;

    public VolumeSurgeDetector(int bucketSeconds, int baselineBuckets, int minBaselineBuckets, DetectionRules rules,
            BaselineSource baselineSource) {
        this.bucketSeconds = bucketSeconds;
        this.baselineBuckets = baselineBuckets;
        this.minBaselineBuckets = minBaselineBuckets;
        this.rules = rules;
        this.baselineSource = baselineSource;
    }

    @Override
    public void open(Configuration parameters) {
        buckets = getRuntimeContext().getListState(new ListStateDescriptor<>("baseline-buckets", Types.DOUBLE));
        lastBucketEnd = getRuntimeContext().getState(new ValueStateDescriptor<>("last-bucket-end", Types.LONG));
        seeded = getRuntimeContext().getState(new ValueStateDescriptor<>("seeded", Types.BOOLEAN));
        lastEmitted = getRuntimeContext().getState(new ValueStateDescriptor<>("last-emitted", Types.LONG));
    }

    @Override
    public void processElement(TradeIntensity window, Context ctx, Collector<DetectionEvent> out) throws Exception {
        if (window.getWindowSeconds() != bucketSeconds) {
            return;
        }
        seedIfNeeded(window.getCode());

        List<Double> history = new ArrayList<>();
        buckets.get().forEach(history::add);

        if (history.size() >= minBaselineBuckets) {
            DetectionRule rule = rules.resolve(window.getCode());
            double baseline = history.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            long now = window.getLastTradeTimestamp();
            Long last = lastEmitted.value();
            boolean coolingDown = last != null && now - last < rule.getVolumeCooldownSeconds() * 1000L;
            if (baseline > 0 && window.getTotalAmount() >= baseline * rule.getVolumeMultiplier() && !coolingDown) {
                out.collect(DetectionEvent.volumeSurge(window, baseline, baselineBuckets * bucketSeconds / 3600));
                lastEmitted.update(now);
            }
        }

        recordAlignedBucket(window, history);
    }

    private void seedIfNeeded(String code) throws Exception {
        if (Boolean.TRUE.equals(seeded.value())) {
            return;
        }
        seeded.update(true);
        List<Double> past = baselineSource.recentBucketAmounts(code, bucketSeconds, baselineBuckets);
        if (!past.isEmpty()) {
            buckets.update(new ArrayList<>(past));
        }
    }

    /** 슬라이딩 출력 중 구간 길이에 맞춰 떨어지는 것만 기준선 버킷으로 삼는다. 그래야 버킷이 겹치지 않는다. */
    private void recordAlignedBucket(TradeIntensity window, List<Double> history) throws Exception {
        long end = window.getWindowEnd();
        if (end % (bucketSeconds * 1000L) != 0) {
            return;
        }
        Long previous = lastBucketEnd.value();
        if (previous != null && end <= previous) {
            return;
        }
        history.add(window.getTotalAmount());
        while (history.size() > baselineBuckets) {
            history.remove(0);
        }
        buckets.update(history);
        lastBucketEnd.update(end);
    }
}
