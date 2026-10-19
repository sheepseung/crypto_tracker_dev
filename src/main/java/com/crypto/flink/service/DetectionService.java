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
import com.crypto.flink.rules.DetectionRules;
import com.crypto.flink.rules.RulesProvider;
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
 * 임계치는 붙이는 시점의 규칙 스냅샷을 싣는다. 구간 길이 같은 구조 설정은 설정 파일에서 읽는다.
 */
@Service
public class DetectionService implements FlinkPipeline {

    private static final Logger logger = LoggerFactory.getLogger(DetectionService.class);

    private final boolean enabled;
    private final int largeBurstWindowSeconds;
    private final int priceWindowSeconds;
    private final int volumeWindowSeconds;
    private final int volumeSlideSeconds;
    private final int volumeBaselineHours;
    private final int volumeMinBaselineBuckets;
    private final RulesProvider rulesProvider;
    private final BaselineSource baselineSource;
    private final KafkaSink<String> eventsSink;

    // 테스트용 생성자가 하나 더 있어 Spring 이 쓸 생성자를 명시한다
    @Autowired
    public DetectionService(
            @Value("${jobs.detection.enabled:true}") boolean enabled,
            @Value("${detection.large-trade.burst-window-seconds:10}") int largeBurstWindowSeconds,
            @Value("${detection.price-move.window-seconds:300}") int priceWindowSeconds,
            @Value("${detection.volume-surge.window-seconds:300}") int volumeWindowSeconds,
            @Value("${detection.volume-surge.slide-seconds:5}") int volumeSlideSeconds,
            @Value("${detection.volume-surge.baseline-hours:24}") int volumeBaselineHours,
            @Value("${detection.volume-surge.min-baseline-buckets:12}") int volumeMinBaselineBuckets,
            @Value("${detection.volume-surge.seed-from-upbit:true}") boolean seedFromUpbit,
            RulesProvider rulesProvider,
            @Qualifier("eventsSink") KafkaSink<String> eventsSink) {
        this(enabled, largeBurstWindowSeconds, priceWindowSeconds, volumeWindowSeconds, volumeSlideSeconds,
                volumeBaselineHours, volumeMinBaselineBuckets, rulesProvider,
                seedFromUpbit ? new UpbitCandleBaselineSource() : BaselineSource.none(), eventsSink);
    }

    DetectionService(boolean enabled, int largeBurstWindowSeconds, int priceWindowSeconds, int volumeWindowSeconds,
            int volumeSlideSeconds, int volumeBaselineHours, int volumeMinBaselineBuckets,
            RulesProvider rulesProvider, BaselineSource baselineSource, KafkaSink<String> eventsSink) {
        this.enabled = enabled;
        this.largeBurstWindowSeconds = largeBurstWindowSeconds;
        this.priceWindowSeconds = priceWindowSeconds;
        this.volumeWindowSeconds = volumeWindowSeconds;
        this.volumeSlideSeconds = volumeSlideSeconds;
        this.volumeBaselineHours = volumeBaselineHours;
        this.volumeMinBaselineBuckets = volumeMinBaselineBuckets;
        this.rulesProvider = rulesProvider;
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
        DetectionRules rules = rulesProvider.current();
        logger.info("감지 파이프라인 연결 - 기본 규칙 {}, 종목 덮어쓰기 {}개, 구간 집중 {}초·급등락 {}초·거래량 {}초/{}시간",
                rules.defaults(), rules.overrides().size(),
                largeBurstWindowSeconds, priceWindowSeconds, volumeWindowSeconds, volumeBaselineHours);
        eventStream(trades, rules)
                .map(new ToJsonMapper<>())
                .name("DetectionEvent to JSON")
                .sinkTo(eventsSink)
                .name("Kafka Sink - Events");
    }

    DataStream<DetectionEvent> eventStream(DataStream<CryptoTrade> trades, DetectionRules rules) {
        DataStream<DetectionEvent> largeTrades = trades
                .keyBy(CryptoTrade::getCode)
                .process(new LargeTradeDetector(rules, largeBurstWindowSeconds))
                .name("Large Trade Detector");

        DataStream<DetectionEvent> priceMoves = trades
                .keyBy(CryptoTrade::getCode)
                .process(new PriceMoveDetector(priceWindowSeconds, rules))
                .name("Price Move Detector");

        return largeTrades.union(priceMoves).union(volumeSurges(trades, rules));
    }

    /**
     * 체결강도 파이프라인과 같은 구간을 한 번 더 집계한다. 파이프라인끼리 결과를 넘기지 않아야
     * 한쪽만 끄거나 바꿔도 다른 쪽이 영향을 받지 않는다. 윈도우 하나라 비용은 작다.
     */
    private DataStream<DetectionEvent> volumeSurges(DataStream<CryptoTrade> trades, DetectionRules rules) {
        DataStream<TradeIntensity> windows = trades
                .keyBy(CryptoTrade::getCode)
                .window(SlidingEventTimeWindows.of(Time.seconds(volumeWindowSeconds), Time.seconds(volumeSlideSeconds)))
                .aggregate(new TradeAmountAggregator(), new IntensityWindowFunction())
                .name("Volume Window " + volumeWindowSeconds + "s");

        int baselineBuckets = volumeBaselineHours * 3600 / volumeWindowSeconds;
        return windows
                .keyBy(TradeIntensity::getCode)
                .process(new VolumeSurgeDetector(volumeWindowSeconds, baselineBuckets, volumeMinBaselineBuckets, rules,
                        baselineSource))
                .name("Volume Surge Detector");
    }
}
