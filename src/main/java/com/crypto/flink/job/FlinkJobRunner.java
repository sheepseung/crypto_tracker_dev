package com.crypto.flink.job;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.function.TradeJsonParser;
import jakarta.annotation.PreDestroy;
import org.apache.flink.api.common.JobID;
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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 체결 스트림을 한 번만 읽어 파싱하고, 켜진 파이프라인을 모두 붙인 뒤 Flink Job 하나로 실행한다.
 * 파이프라인마다 Job 을 띄우면 같은 토픽을 여러 번 읽고 실행 환경도 충돌하므로 여기서만 실행한다.
 * 규칙이 바뀌면 Job 만 취소하고 새 환경으로 다시 띄운다.
 */
@Component
public class FlinkJobRunner implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(FlinkJobRunner.class);
    private static final String JOB_NAME = "crypto-tracker";
    private static final long CANCEL_TIMEOUT_SECONDS = 30;

    // 업비트는 종목별로는 순서를 지키지만 종목 사이에서는 체결 시각이 조금 엇갈려 들어온다
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(2);
    // 파티션 하나가 조용해도 다른 파티션의 워터마크가 멈추지 않도록 한다
    private static final Duration SOURCE_IDLE_TIMEOUT = Duration.ofSeconds(30);

    private final ObjectProvider<StreamExecutionEnvironment> envProvider;
    private final KafkaSource<String> kafkaSource;
    private final KafkaSink<String> deadLetterSink;
    private final List<FlinkPipeline> pipelines;

    private final Object lock = new Object();
    private JobClient jobClient;

    public FlinkJobRunner(ObjectProvider<StreamExecutionEnvironment> envProvider,
            KafkaSource<String> kafkaSource,
            @Qualifier("deadLetterSink") KafkaSink<String> deadLetterSink,
            List<FlinkPipeline> pipelines) {
        this.envProvider = envProvider;
        this.kafkaSource = kafkaSource;
        this.deadLetterSink = deadLetterSink;
        this.pipelines = pipelines;
    }

    @Override
    public void run(ApplicationArguments args) {
        synchronized (lock) {
            launch();
        }
    }

    /** 현재 Job 을 취소하고 새 환경에 파이프라인을 다시 붙여 띄운다. 파이프라인은 이때 최신 규칙을 읽는다. */
    public void restart() {
        synchronized (lock) {
            stopJob();
            launch();
        }
    }

    public Optional<JobID> currentJobId() {
        synchronized (lock) {
            return Optional.ofNullable(jobClient).map(JobClient::getJobID);
        }
    }

    @PreDestroy
    public void stop() {
        synchronized (lock) {
            stopJob();
        }
    }

    private void launch() {
        List<FlinkPipeline> enabled = pipelines.stream().filter(FlinkPipeline::isEnabled).toList();
        if (enabled.isEmpty()) {
            logger.warn("켜진 파이프라인이 없어 Flink Job 을 시작하지 않습니다 (jobs.*.enabled 확인)");
            return;
        }

        // 환경에는 붙인 연산이 쌓이므로 띄울 때마다 새로 만든다
        StreamExecutionEnvironment env = envProvider.getObject();
        DataStream<CryptoTrade> trades = buildTradeStream(env);
        for (FlinkPipeline pipeline : enabled) {
            pipeline.attach(trades);
        }

        try {
            // execute() 는 Job 이 끝날 때까지 돌아오지 않아 Spring 기동을 막으므로 비동기로 띄운다
            jobClient = env.executeAsync(JOB_NAME);
        } catch (Exception e) {
            throw new IllegalStateException("Flink Job 시작 실패", e);
        }
        logger.info("Flink Job 시작: jobId={}, pipelines={}", jobClient.getJobID(),
                enabled.stream().map(FlinkPipeline::name).toList());
    }

    private void stopJob() {
        if (jobClient == null) {
            return;
        }
        JobID jobId = jobClient.getJobID();
        try {
            jobClient.cancel().get(CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            logger.info("Flink Job 취소: jobId={}", jobId);
        } catch (Exception e) {
            logger.warn("Flink Job 취소 중 오류 (jobId={}): {}", jobId, e.toString());
        }
        jobClient = null;
    }

    private DataStream<CryptoTrade> buildTradeStream(StreamExecutionEnvironment env) {
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
