package com.crypto.flink.service;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.function.BaselineSource;
import com.crypto.flink.function.IntensityWindowFunction;
import com.crypto.flink.function.LargeTradeDetector;
import com.crypto.flink.function.PriceMoveDetector;
import com.crypto.flink.function.ToJsonMapper;
import com.crypto.flink.function.TradeAmountAggregator;
import com.crypto.flink.function.UpbitCandleBaselineSource;
import com.crypto.flink.function.VolumeSurgeDetector;
import com.crypto.flink.job.FlinkPipeline;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 대량 체결(F2), 급등·급락(F3), 거래량 급증(F5)을 감지해 events 토픽에 쓴다.
 */
@Service
public class DetectionService implements FlinkPipeline {

    private static final Logger logger = LoggerFactory.getLogger(DetectionService.class);

    private final boolean enabled;
    private final double largeSingleAmount;
    private final double largeBurstAmount;
    private final int largeBurstWindowSeconds;
    private final int largeCooldownSeconds;
    private final int priceWindowSeconds;
    private final double priceThresholdPercent;
    private final int priceCooldownSeconds;
    private final int volumeWindowSeconds;
    private final int volumeSlideSeconds;
    private final int volumeBaselineHours;
    private final int volumeMinBaselineBuckets;
    private final double volumeMultiplier;
    private final int volumeCooldownSeconds;
    private final BaselineSource baselineSource;
    private final KafkaSink<String> eventsSink;

    // 테스트용 생성자가 하나 더 있어 Spring 이 쓸 생성자를 명시한다
    @Autowired
    public DetectionService(
            @Value("${jobs.detection.enabled:true}") boolean enabled,
            @Value("${detection.large-trade.single-amount:300000000}") double largeSingleAmount,
            @Value("${detection.large-trade.burst-amount:300000000}") double largeBurstAmount,
            @Value("${detection.large-trade.burst-window-seconds:10}") int largeBurstWindowSeconds,
            @Value("${detection.large-trade.cooldown-seconds:60}") int largeCooldownSeconds,
            @Value("${detection.price-move.window-seconds:300}") int priceWindowSeconds,
            @Value("${detection.price-move.threshold-percent:2.0}") double priceThresholdPercent,
            @Value("${detection.price-move.cooldown-seconds:300}") int priceCooldownSeconds,
            @Value("${detection.volume-surge.window-seconds:300}") int volumeWindowSeconds,
            @Value("${detection.volume-surge.slide-seconds:5}") int volumeSlideSeconds,
            @Value("${detection.volume-surge.baseline-hours:24}") int volumeBaselineHours,
            @Value("${detection.volume-surge.min-baseline-buckets:12}") int volumeMinBaselineBuckets,
            @Value("${detection.volume-surge.multiplier:5.0}") double volumeMultiplier,
            @Value("${detection.volume-surge.cooldown-seconds:300}") int volumeCooldownSeconds,
            @Value("${detection.volume-surge.seed-from-upbit:true}") boolean seedFromUpbit,
            @Qualifier("eventsSink") KafkaSink<String> eventsSink) {
        this(enabled, largeSingleAmount, largeBurstAmount, largeBurstWindowSeconds, largeCooldownSeconds,
                priceWindowSeconds, priceThresholdPercent, priceCooldownSeconds,
                volumeWindowSeconds, volumeSlideSeconds, volumeBaselineHours, volumeMinBaselineBuckets,
                volumeMultiplier, volumeCooldownSeconds,
                seedFromUpbit ? new UpbitCandleBaselineSource() : BaselineSource.none(), eventsSink);
    }

    DetectionService(boolean enabled, double largeSingleAmount, double largeBurstAmount, int largeBurstWindowSeconds,
            int largeCooldownSeconds, int priceWindowSeconds, double priceThresholdPercent, int priceCooldownSeconds,
            int volumeWindowSeconds, int volumeSlideSeconds, int volumeBaselineHours, int volumeMinBaselineBuckets,
            double volumeMultiplier, int volumeCooldownSeconds, BaselineSource baselineSource,
            KafkaSink<String> eventsSink) {
        this.enabled = enabled;
        this.largeSingleAmount = largeSingleAmount;
        this.largeBurstAmount = largeBurstAmount;
        this.largeBurstWindowSeconds = largeBurstWindowSeconds;
        this.largeCooldownSeconds = largeCooldownSeconds;
        this.priceWindowSeconds = priceWindowSeconds;
        this.priceThresholdPercent = priceThresholdPercent;
        this.priceCooldownSeconds = priceCooldownSeconds;
        this.volumeWindowSeconds = volumeWindowSeconds;
        this.volumeSlideSeconds = volumeSlideSeconds;
        this.volumeBaselineHours = volumeBaselineHours;
        this.volumeMinBaselineBuckets = volumeMinBaselineBuckets;
        this.volumeMultiplier = volumeMultiplier;
        this.volumeCooldownSeconds = volumeCooldownSeconds;
        this.baselineSource = baselineSource;
        this.eventsSink = eventsSink;
    }

    @Override
    public String name() {
        return "detection";
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void attach(DataStream<CryptoTrade> trades) {
        logger.info("감지 파이프라인 연결 - 대량 체결 단일 {}원·{}초 집중 {}원, 급등락 {}초 ±{}%, 거래량 급증 {}초 구간 {}시간 평균의 {}배",
                (long) largeSingleAmount, largeBurstWindowSeconds, (long) largeBurstAmount,
                priceWindowSeconds, priceThresholdPercent,
                volumeWindowSeconds, volumeBaselineHours, volumeMultiplier);
        eventStream(trades)
                .map(new ToJsonMapper<>())
                .name("DetectionEvent to JSON")
                .sinkTo(eventsSink)
                .name("Kafka Sink - Events");
    }

    DataStream<DetectionEvent> eventStream(DataStream<CryptoTrade> trades) {
        DataStream<DetectionEvent> largeTrades = trades
                .keyBy(CryptoTrade::getCode)
                .process(new LargeTradeDetector(largeSingleAmount, largeBurstAmount, largeBurstWindowSeconds,
                        largeCooldownSeconds))
                .name("Large Trade Detector");

        DataStream<DetectionEvent> priceMoves = trades
                .keyBy(CryptoTrade::getCode)
                .process(new PriceMoveDetector(priceWindowSeconds, priceThresholdPercent, priceCooldownSeconds))
                .name("Price Move Detector");

        return largeTrades.union(priceMoves).union(volumeSurges(trades));
    }

    /**
     * 체결강도 파이프라인과 같은 구간을 한 번 더 집계한다. 파이프라인끼리 결과를 넘기지 않아야
     * 한쪽만 끄거나 바꿔도 다른 쪽이 영향을 받지 않는다. 윈도우 하나라 비용은 작다.
     */
    private DataStream<DetectionEvent> volumeSurges(DataStream<CryptoTrade> trades) {
        DataStream<TradeIntensity> windows = trades
                .keyBy(CryptoTrade::getCode)
                .window(SlidingEventTimeWindows.of(Time.seconds(volumeWindowSeconds), Time.seconds(volumeSlideSeconds)))
                .aggregate(new TradeAmountAggregator(), new IntensityWindowFunction())
                .name("Volume Window " + volumeWindowSeconds + "s");

        int baselineBuckets = volumeBaselineHours * 3600 / volumeWindowSeconds;
        return windows
                .keyBy(TradeIntensity::getCode)
                .process(new VolumeSurgeDetector(volumeWindowSeconds, baselineBuckets, volumeMinBaselineBuckets,
                        volumeMultiplier, volumeCooldownSeconds, baselineSource))
                .name("Volume Surge Detector");
    }
}
