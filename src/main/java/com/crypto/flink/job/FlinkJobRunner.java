package com.crypto.flink.job;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.function.TradeJsonParser;
import jakarta.annotation.PreDestroy;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Kafka 체결 스트림을 한 번만 읽어 파싱하고, 켜진 파이프라인을 모두 붙인 뒤 Flink Job 하나로 실행한다.
 * 파이프라인마다 Job 을 띄우면 같은 토픽을 여러 번 읽고 실행 환경도 충돌하므로 여기서만 실행한다.
 */
@Component
public class FlinkJobRunner implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(FlinkJobRunner.class);
    private static final String JOB_NAME = "crypto-tracker";

    // 업비트는 종목별로는 순서를 지키지만 종목 사이에서는 체결 시각이 조금 엇갈려 들어온다
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(2);
    // 파티션 하나가 조용해도 다른 파티션의 워터마크가 멈추지 않도록 한다
    private static final Duration SOURCE_IDLE_TIMEOUT = Duration.ofSeconds(30);

    private final StreamExecutionEnvironment env;
    private final KafkaSource<String> kafkaSource;
    private final KafkaSink<String> deadLetterSink;
    private final List<FlinkPipeline> pipelines;

    private JobClient jobClient;

    public FlinkJobRunner(StreamExecutionEnvironment env,
            KafkaSource<String> kafkaSource,
            @Qualifier("deadLetterSink") KafkaSink<String> deadLetterSink,
            List<FlinkPipeline> pipelines) {
        this.env = env;
        this.kafkaSource = kafkaSource;
        this.deadLetterSink = deadLetterSink;
        this.pipelines = pipelines;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<FlinkPipeline> enabled = pipelines.stream().filter(FlinkPipeline::isEnabled).toList();
        if (enabled.isEmpty()) {
            logger.warn("켜진 파이프라인이 없어 Flink Job 을 시작하지 않습니다 (jobs.*.enabled 확인)");
            return;
        }

        DataStream<CryptoTrade> trades = buildTradeStream();
        for (FlinkPipeline pipeline : enabled) {
            pipeline.attach(trades);
        }

        // execute() 는 Job 이 끝날 때까지 돌아오지 않아 Spring 기동을 막으므로 비동기로 띄운다
        jobClient = env.executeAsync(JOB_NAME);
        logger.info("Flink Job 시작: jobId={}, pipelines={}", jobClient.getJobID(),
                enabled.stream().map(FlinkPipeline::name).toList());
    }

    @PreDestroy
    public void stop() {
        if (jobClient != null) {
            jobClient.cancel();
        }
    }

    private DataStream<CryptoTrade> buildTradeStream() {
        DataStream<String> inputStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source - crypto-trades");

        SingleOutputStreamOperator<CryptoTrade> tradeStream = inputStream
                .process(new TradeJsonParser())
                .name("JSON to CryptoTrade");

        tradeStream.getSideOutput(TradeJsonParser.PARSE_FAILURES)
                .sinkTo(deadLetterSink)
                .name("Dead Letter");

        return withEventTime(tradeStream);
    }

    /** 체결 시각 기준으로 집계해야 재시작 후 Kafka 에 쌓인 체결을 다시 읽어도 구간이 밀리지 않는다. */
    public static DataStream<CryptoTrade> withEventTime(DataStream<CryptoTrade> trades) {
        return trades.assignTimestampsAndWatermarks(
                WatermarkStrategy.<CryptoTrade>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<CryptoTrade>) (trade, ts) -> trade.getTradeTimestamp())
                        .withIdleness(SOURCE_IDLE_TIMEOUT));
    }
}
