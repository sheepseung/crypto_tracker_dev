package com.crypto.flink.service;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.function.IntensityWindowFunction;
import com.crypto.flink.function.ToJsonMapper;
import com.crypto.flink.function.TradeAmountAggregator;
import com.crypto.flink.job.FlinkPipeline;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 종목별 체결강도·순매수 지표(F1)를 여러 구간으로 집계해 metrics 토픽에 쓴다. 주체별(고래·중형·개미) 금액도 같이 싣는다.
 */
@Service
public class TradeIntensityService implements FlinkPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TradeIntensityService.class);

    private final List<Integer> windowsSeconds;
    private final int slideSeconds;
    private final boolean enabled;
    private final double whaleMinAmount;
    private final double retailMaxAmount;
    private final KafkaSink<String> metricsSink;

    public TradeIntensityService(
            @Value("${trade.intensity.windows-seconds:60,300,900}") List<Integer> windowsSeconds,
            @Value("${trade.intensity.slide-seconds:5}") int slideSeconds,
            @Value("${jobs.tradeIntensity.enabled:true}") boolean enabled,
            @Value("${tiers.whale-min-amount:10000000}") double whaleMinAmount,
            @Value("${tiers.retail-max-amount:100000}") double retailMaxAmount,
            @Qualifier("metricsSink") KafkaSink<String> metricsSink) {
        this.windowsSeconds = windowsSeconds;
        this.slideSeconds = slideSeconds;
        this.enabled = enabled;
        this.whaleMinAmount = whaleMinAmount;
        this.retailMaxAmount = retailMaxAmount;
        this.metricsSink = metricsSink;
    }

    @Override
    public String name() {
        return "tradeIntensity";
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void attach(DataStream<CryptoTrade> trades) {
        logger.info("체결강도 파이프라인 연결 - 구간 {}초, 갱신 {}초, 고래 {}원 이상, 개미 {}원 미만",
                windowsSeconds, slideSeconds, (long) whaleMinAmount, (long) retailMaxAmount);
        intensityStream(trades)
                .map(new ToJsonMapper<>())
                .name("TradeIntensity to JSON")
                .sinkTo(metricsSink)
                .name("Kafka Sink - Metrics");
    }

    /** 구간마다 슬라이딩 윈도우로 집계한 결과를 하나의 스트림으로 합친다. */
    DataStream<TradeIntensity> intensityStream(DataStream<CryptoTrade> trades) {
        DataStream<TradeIntensity> merged = null;
        for (int windowSeconds : windowsSeconds) {
            DataStream<TradeIntensity> windowed = trades
                    .keyBy(CryptoTrade::getCode)
                    .window(SlidingEventTimeWindows.of(Time.seconds(windowSeconds), Time.seconds(slideSeconds)))
                    .aggregate(new TradeAmountAggregator(whaleMinAmount, retailMaxAmount), new IntensityWindowFunction())
                    .name("Trade Intensity " + windowSeconds + "s");
            merged = merged == null ? windowed : merged.union(windowed);
        }
        return merged;
    }
}
