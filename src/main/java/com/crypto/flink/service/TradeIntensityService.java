package com.crypto.flink.service;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.function.IntensityWindowFunction;
import com.crypto.flink.function.ToJsonMapper;
import com.crypto.flink.function.TradeAmountAggregator;
import com.crypto.flink.function.TradeJsonParser;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 종목별 체결강도·순매수 지표(F1)를 여러 구간으로 집계해 metrics 토픽에 쓴다.
 */
@Service
public class TradeIntensityService {

    private static final Logger logger = LoggerFactory.getLogger(TradeIntensityService.class);

    // 업비트는 종목별로는 순서를 지키지만 종목 사이에서는 체결 시각이 조금 엇갈려 들어온다
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(2);
    // 파티션 하나가 조용해도 다른 파티션의 워터마크가 멈추지 않도록 한다
    private static final Duration SOURCE_IDLE_TIMEOUT = Duration.ofSeconds(30);

    @Value("${trade.intensity.windows-seconds:60,300,900}")
    private List<Integer> windowsSeconds;

    @Value("${trade.intensity.slide-seconds:5}")
    private int slideSeconds;

    @Value("${jobs.tradeIntensity.enabled:false}")
    private boolean jobEnabled;

    @Autowired
    private StreamExecutionEnvironment env;

    @Autowired
    private KafkaSource<String> kafkaSource;

    @Autowired
    @Qualifier("metricsSink")
    private KafkaSink<String> metricsSink;

    @Autowired
    @Qualifier("deadLetterSink")
    private KafkaSink<String> deadLetterSink;

    @PostConstruct
    public void init() {
        if (!jobEnabled) {
            logger.info("TradeIntensityService 비활성화됨 (jobs.tradeIntensity.enabled=false)");
            return;
        }
        // execute() 는 Job 이 끝날 때까지 돌아오지 않으므로 Spring 기동을 막지 않게 따로 돌린다
        CompletableFuture.runAsync(this::run);
    }

    void run() {
        try {
            logger.info("체결강도 Job 시작 - 구간 {}초, 갱신 {}초", windowsSeconds, slideSeconds);
            buildPipeline();
            env.execute("Trade Intensity Job");
        } catch (Exception e) {
            logger.error("체결강도 Job 실행 중 오류", e);
        }
    }

    void buildPipeline() {
        DataStream<String> inputStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source - Trade Intensity");

        SingleOutputStreamOperator<CryptoTrade> tradeStream = inputStream
                .process(new TradeJsonParser())
                .name("JSON to CryptoTrade - Trade Intensity");

        tradeStream.getSideOutput(TradeJsonParser.PARSE_FAILURES)
                .sinkTo(deadLetterSink)
                .name("Dead Letter - Trade Intensity");

        // 체결 시각 기준으로 집계해야 재시작 후 Kafka 에 쌓인 체결을 다시 읽어도 구간이 밀리지 않는다
        DataStream<CryptoTrade> timedStream = tradeStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<CryptoTrade>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<CryptoTrade>) (trade, ts) -> trade.getTradeTimestamp())
                        .withIdleness(SOURCE_IDLE_TIMEOUT));

        DataStream<TradeIntensity> intensityStream = null;
        for (int windowSeconds : windowsSeconds) {
            DataStream<TradeIntensity> windowed = timedStream
                    .keyBy(CryptoTrade::getCode)
                    .window(SlidingEventTimeWindows.of(Time.seconds(windowSeconds), Time.seconds(slideSeconds)))
                    .aggregate(new TradeAmountAggregator(), new IntensityWindowFunction())
                    .name("Trade Intensity " + windowSeconds + "s");
            intensityStream = intensityStream == null ? windowed : intensityStream.union(windowed);
        }

        intensityStream
                .map(new ToJsonMapper<>())
                .name("TradeIntensity to JSON")
                .sinkTo(metricsSink)
                .name("Kafka Sink - Metrics");
    }
}
