package com.crypto.flink.service;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.function.LargeTradeDetector;
import com.crypto.flink.function.PriceMoveDetector;
import com.crypto.flink.function.ToJsonMapper;
import com.crypto.flink.job.FlinkPipeline;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 대량 체결(F2)과 급등·급락(F3)을 감지해 events 토픽에 쓴다.
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
    private final KafkaSink<String> eventsSink;

    public DetectionService(
            @Value("${jobs.detection.enabled:true}") boolean enabled,
            @Value("${detection.large-trade.single-amount:300000000}") double largeSingleAmount,
            @Value("${detection.large-trade.burst-amount:300000000}") double largeBurstAmount,
            @Value("${detection.large-trade.burst-window-seconds:10}") int largeBurstWindowSeconds,
            @Value("${detection.large-trade.cooldown-seconds:60}") int largeCooldownSeconds,
            @Value("${detection.price-move.window-seconds:300}") int priceWindowSeconds,
            @Value("${detection.price-move.threshold-percent:2.0}") double priceThresholdPercent,
            @Value("${detection.price-move.cooldown-seconds:300}") int priceCooldownSeconds,
            @Qualifier("eventsSink") KafkaSink<String> eventsSink) {
        this.enabled = enabled;
        this.largeSingleAmount = largeSingleAmount;
        this.largeBurstAmount = largeBurstAmount;
        this.largeBurstWindowSeconds = largeBurstWindowSeconds;
        this.largeCooldownSeconds = largeCooldownSeconds;
        this.priceWindowSeconds = priceWindowSeconds;
        this.priceThresholdPercent = priceThresholdPercent;
        this.priceCooldownSeconds = priceCooldownSeconds;
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
        logger.info("감지 파이프라인 연결 - 대량 체결 단일 {}원·{}초 집중 {}원, 급등락 {}초 ±{}%",
                (long) largeSingleAmount, largeBurstWindowSeconds, (long) largeBurstAmount,
                priceWindowSeconds, priceThresholdPercent);
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

        return largeTrades.union(priceMoves);
    }
}
