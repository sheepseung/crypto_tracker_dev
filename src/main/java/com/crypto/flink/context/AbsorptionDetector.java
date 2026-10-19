package com.crypto.flink.context;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.store.IntensityBucketRepository.Bucket;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * G5 흡수·소진. 5분 버킷의 순매수·거래대금을 그 종목의 평소 분포(백분위)와 가격 변화에 같이 놓고 본다.
 * 흡수(ABSORPTION): 순매수가 평소 상위(하위) 몇 % 안인데 가격은 거의 안 움직였다. 반대쪽이 받아 주고 있다는 뜻이다.
 * 유동성 공백(LIQUIDITY_GAP): 가격은 크게 움직였는데 거래대금은 평소 하위권이다. 얇은 호가를 뚫은 움직임이라 되돌림이 잦다.
 * 종목 규모와 무관하게 백분위로 보므로 임계치가 하나다. 분포 표본이 부족하면 판정하지 않는다.
 */
@Component
public class AbsorptionDetector {

    private final double netPercentile;
    private final double maxFlatMovePercent;
    private final double gapMinMovePercent;
    private final double gapMaxAmountPercentile;
    private final int minSamples;
    private final long cooldownMillis;
    private final Function<Bucket, Optional<ContextService.CodeContext>> percentiles;

    private final Map<String, Long> lastEmitted = new ConcurrentHashMap<>();

    // 테스트용 생성자가 하나 더 있어 Spring 이 쓸 생성자를 명시한다
    @Autowired
    public AbsorptionDetector(ContextService context,
            @Value("${absorption.net-percentile:95}") double netPercentile,
            @Value("${absorption.max-flat-move-percent:0.1}") double maxFlatMovePercent,
            @Value("${absorption.gap-min-move-percent:1.0}") double gapMinMovePercent,
            @Value("${absorption.gap-max-amount-percentile:30}") double gapMaxAmountPercentile,
            @Value("${absorption.min-samples:100}") int minSamples,
            @Value("${absorption.cooldown-seconds:900}") int cooldownSeconds) {
        this(b -> context.contextFor(b.code(), b.intensity(), b.totalAmount(), b.netAmount()),
                netPercentile, maxFlatMovePercent, gapMinMovePercent, gapMaxAmountPercentile, minSamples, cooldownSeconds);
    }

    AbsorptionDetector(Function<Bucket, Optional<ContextService.CodeContext>> percentiles, double netPercentile,
            double maxFlatMovePercent, double gapMinMovePercent, double gapMaxAmountPercentile, int minSamples,
            int cooldownSeconds) {
        this.percentiles = percentiles;
        this.netPercentile = netPercentile;
        this.maxFlatMovePercent = maxFlatMovePercent;
        this.gapMinMovePercent = gapMinMovePercent;
        this.gapMaxAmountPercentile = gapMaxAmountPercentile;
        this.minSamples = minSamples;
        this.cooldownMillis = cooldownSeconds * 1000L;
    }

    public List<DetectionEvent> onBucket(Bucket b) {
        List<DetectionEvent> events = new ArrayList<>();
        if (b.openPrice() <= 0 || b.closePrice() <= 0) {
            return events;
        }
        ContextService.CodeContext ctx = percentiles.apply(b).orElse(null);
        if (ctx == null || ctx.samples() < minSamples || ctx.netAmountPercentile() == null) {
            return events;
        }
        double move = b.priceChangePercent();
        double netPct = ctx.netAmountPercentile();

        boolean strongBuy = netPct >= netPercentile;
        boolean strongSell = netPct <= 100 - netPercentile;
        if ((strongBuy || strongSell) && Math.abs(move) < maxFlatMovePercent
                && cooldownPassed(b.code(), DetectionEvent.ABSORPTION, b.windowEnd())) {
            events.add(DetectionEvent.absorption(b, strongBuy, netPct, move));
        }

        if (Math.abs(move) >= gapMinMovePercent && ctx.totalAmountPercentile() <= gapMaxAmountPercentile
                && cooldownPassed(b.code(), DetectionEvent.LIQUIDITY_GAP, b.windowEnd())) {
            events.add(DetectionEvent.liquidityGap(b, ctx.totalAmountPercentile(), move));
        }
        return events;
    }

    private boolean cooldownPassed(String code, String type, long now) {
        String key = code + "|" + type;
        Long last = lastEmitted.get(key);
        if (last != null && now - last < cooldownMillis) {
            return false;
        }
        lastEmitted.put(key, now);
        return true;
    }
}
