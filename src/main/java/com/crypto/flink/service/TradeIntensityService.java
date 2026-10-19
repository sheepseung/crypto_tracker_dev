package com.crypto.flink.service;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.function.TradeJsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 실시간 체결강도(Trade Intensity) 계산 Flink 스트리밍 서비스
 * 
 * 주요 기능:
 * - 슬라이딩 윈도우를 사용한 실시간 체결량 집계
 * - 체결강도 = (총 매수 체결량 / 총 매도 체결량) * 100
 * - 시장 심리 및 트렌드 방향 분석
 * - 체결강도 변화에 따른 실시간 알림 생성
 * - WebSocket을 통한 프론트엔드 스트리밍
 */
@Service
public class TradeIntensityService {

    private static final Logger logger = LoggerFactory.getLogger(TradeIntensityService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${trade.intensity.window.size:60}")
    private Integer windowSizeSeconds;

    @Value("${trade.intensity.window.slide:5}")
    private Integer windowSlideSeconds;

    @Value("${trade.intensity.alert.bullish:150}")
    private Double bullishThreshold;

    @Value("${trade.intensity.alert.bearish:70}")
    private Double bearishThreshold;

    @Value("${trade.intensity.alert.volatility:50}")
    private Double volatilityThreshold;

    @Value("${jobs.tradeIntensity.enabled:false}")
    private boolean jobEnabled;

    @Autowired
    private StreamExecutionEnvironment env;

    @Autowired
    private KafkaSource<String> kafkaSource;

    @Autowired
    @Qualifier("kafkaSink")
    private KafkaSink<String> kafkaSink;

    @Autowired
    @Qualifier("deadLetterSink")
    private KafkaSink<String> deadLetterSink;

    /**
     * 체결강도 계산 Flink Job 시작
     */
    @PostConstruct
    public void startTradeIntensityJob() {
        try {
            if (!jobEnabled) {
                logger.info("⏸️ TradeIntensityService 비활성화됨 (jobs.tradeIntensity.enabled=false)");
                return;
            }
            logger.info("📊 체결강도 계산 Flink Job을 시작합니다...");
            logger.info("📈 설정값 - 윈도우 크기: {}초, 슬라이드: {}초", windowSizeSeconds, windowSlideSeconds);
            logger.info("🚨 알림 임계치 - 매수세: {}, 매도세: {}, 변동성: {}",
                    bullishThreshold, bearishThreshold, volatilityThreshold);

            // Kafka 소스에서 데이터 스트림 생성
            DataStream<String> inputStream = env.fromSource(
                    kafkaSource,
                    org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                    "Kafka Source - Trade Intensity");

            // JSON 문자열을 CryptoTrade 객체로 변환
            SingleOutputStreamOperator<CryptoTrade> tradeStream = inputStream
                    .process(new TradeJsonParser())
                    .name("JSON to CryptoTrade - Trade Intensity");

            tradeStream.getSideOutput(TradeJsonParser.PARSE_FAILURES)
                    .sinkTo(deadLetterSink)
                    .name("Dead Letter - Trade Intensity");

            // 슬라이딩 윈도우를 사용한 체결량 집계
            DataStream<TradeIntensity> intensityStream = tradeStream
                    .keyBy(CryptoTrade::getCode)
                    .window(SlidingProcessingTimeWindows.of(
                            Time.seconds(windowSizeSeconds),
                            Time.seconds(windowSlideSeconds)))
                    .aggregate(new TradeVolumeAggregator())
                    .name("Trade Volume Aggregation");

            // 체결강도 계산 및 분석
            DataStream<TradeIntensity> analyzedStream = intensityStream
                    .map(new IntensityAnalyzer())
                    .name("Intensity Analysis");

            // 분석 결과를 JSON 문자열로 변환
            DataStream<String> outputStream = analyzedStream
                    .map(new TradeIntensityToJsonMapper())
                    .name("TradeIntensity to JSON");

            // Kafka로 결과 전송
            outputStream.sinkTo(kafkaSink).name("Kafka Sink - Trade Intensity");

            // Job 실행
            env.execute("Trade Intensity Calculation Job");

        } catch (Exception e) {
            logger.error("❌ 체결강도 Job 실행 중 오류 발생: ", e);
        }
    }

    /**
     * 체결량을 집계하는 AggregateFunction
     */
    public static class TradeVolumeAggregator
            implements AggregateFunction<CryptoTrade, Tuple3<Double, Double, Integer>, TradeIntensity> {

        @Override
        public Tuple3<Double, Double, Integer> createAccumulator() {
            return new Tuple3<>(0.0, 0.0, 0);
        }

        @Override
        public Tuple3<Double, Double, Integer> add(CryptoTrade trade,
                Tuple3<Double, Double, Integer> accumulator) {

            Double buyVolume = accumulator.f0;
            Double sellVolume = accumulator.f1;
            Integer count = accumulator.f2;

            if (trade.isBid()) {
                buyVolume += trade.getTradeVolume();
            } else {
                sellVolume += trade.getTradeVolume();
            }

            return new Tuple3<>(buyVolume, sellVolume, count + 1);
        }

        @Override
        public TradeIntensity getResult(Tuple3<Double, Double, Integer> accumulator) {
            Double buyVolume = accumulator.f0;
            Double sellVolume = accumulator.f1;
            Integer count = accumulator.f2;
            Double totalVolume = buyVolume + sellVolume;

            // 체결강도 계산
            Double intensityScore = 100.0;
            if (sellVolume > 0) {
                intensityScore = (buyVolume / sellVolume) * 100;
            }

            // 기본값 반환 (실제로는 윈도우 정보가 필요)
            return new TradeIntensity(
                    LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    "UNKNOWN",
                    LocalDateTime.now().minusSeconds(60).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    60,
                    buyVolume,
                    sellVolume,
                    totalVolume,
                    intensityScore,
                    intensityScore,
                    "NEUTRAL",
                    "MEDIUM",
                    "STABLE",
                    0.0,
                    "LOW",
                    "정상 범위",
                    false);
        }

        @Override
        public Tuple3<Double, Double, Integer> merge(Tuple3<Double, Double, Integer> a,
                Tuple3<Double, Double, Integer> b) {
            return new Tuple3<>(a.f0 + b.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }

    /**
     * 체결강도를 분석하는 Mapper
     */
    public static class IntensityAnalyzer extends RichMapFunction<TradeIntensity, TradeIntensity> {

        @Override
        public TradeIntensity map(TradeIntensity intensity) throws Exception {

            // 체결강도 기반 시장 심리 분석
            String marketSentiment = determineMarketSentiment(intensity.getIntensityScore());
            String intensityLevel = determineIntensityLevel(intensity.getIntensityScore());
            String trendDirection = determineTrendDirection(intensity);
            Double trendStrength = calculateTrendStrength(intensity);

            // 알림 생성
            String alertLevel = determineAlertLevel(intensity);
            String alertMessage = generateAlertMessage(intensity, marketSentiment);
            Boolean isAlertTriggered = shouldTriggerAlert(intensity);

            // 분석 결과 업데이트
            intensity.setMarketSentiment(marketSentiment);
            intensity.setIntensityLevel(intensityLevel);
            intensity.setTrendDirection(trendDirection);
            intensity.setTrendStrength(trendStrength);
            intensity.setAlertLevel(alertLevel);
            intensity.setAlertMessage(alertMessage);
            intensity.setIsAlertTriggered(isAlertTriggered);

            // 로그 출력
            if (isAlertTriggered) {
                logger.info("🚨 체결강도 알림: {} - {} - 강도: {:.2f} - 심리: {} - 알림: {}",
                        intensity.getCode(), intensity.getIntensityDescription(),
                        intensity.getIntensityScore(), marketSentiment, alertLevel);
            } else {
                logger.info("📊 체결강도 분석: {} - {} - 강도: {:.2f} - 심리: {}",
                        intensity.getCode(), intensity.getIntensityDescription(),
                        intensity.getIntensityScore(), marketSentiment);
            }

            return intensity;
        }

        /**
         * 시장 심리 결정
         */
        private String determineMarketSentiment(Double intensityScore) {
            if (intensityScore >= 120)
                return "BULLISH";
            if (intensityScore <= 80)
                return "BEARISH";
            return "NEUTRAL";
        }

        /**
         * 강도 레벨 결정
         */
        private String determineIntensityLevel(Double intensityScore) {
            if (intensityScore >= 150)
                return "VERY_HIGH";
            if (intensityScore >= 120)
                return "HIGH";
            if (intensityScore >= 90)
                return "MEDIUM";
            if (intensityScore >= 60)
                return "LOW";
            return "VERY_LOW";
        }

        /**
         * 트렌드 방향 결정
         */
        private String determineTrendDirection(TradeIntensity intensity) {
            // 실제로는 이전 값과 비교해야 함 (간단한 예시)
            Double intensityScore = intensity.getIntensityScore();

            if (intensityScore > 110)
                return "RISING";
            if (intensityScore < 90)
                return "FALLING";
            return "STABLE";
        }

        /**
         * 트렌드 강도 계산
         */
        private Double calculateTrendStrength(TradeIntensity intensity) {
            Double intensityScore = intensity.getIntensityScore();

            if (intensityScore >= 150 || intensityScore <= 50)
                return 100.0;
            if (intensityScore >= 130 || intensityScore <= 70)
                return 80.0;
            if (intensityScore >= 110 || intensityScore <= 90)
                return 60.0;
            return 30.0;
        }

        /**
         * 알림 레벨 결정
         */
        private String determineAlertLevel(TradeIntensity intensity) {
            Double intensityScore = intensity.getIntensityScore();
            Double totalVolume = intensity.getTotalVolume();

            if (intensityScore >= 200 || intensityScore <= 30 || totalVolume > 10.0) {
                return "CRITICAL";
            } else if (intensityScore >= 150 || intensityScore <= 50 || totalVolume > 5.0) {
                return "HIGH";
            } else if (intensityScore >= 120 || intensityScore <= 80 || totalVolume > 2.0) {
                return "MEDIUM";
            }
            return "LOW";
        }

        /**
         * 알림 메시지 생성
         */
        private String generateAlertMessage(TradeIntensity intensity, String marketSentiment) {
            Double intensityScore = intensity.getIntensityScore();

            if (intensityScore >= 150) {
                return "매우 강한 매수세가 관찰되고 있습니다!";
            } else if (intensityScore <= 50) {
                return "매우 강한 매도세가 관찰되고 있습니다!";
            } else if (intensityScore >= 120) {
                return "강한 매수세가 지속되고 있습니다.";
            } else if (intensityScore <= 80) {
                return "강한 매도세가 지속되고 있습니다.";
            }
            return "시장이 안정적으로 유지되고 있습니다.";
        }

        /**
         * 알림 발생 여부 결정
         */
        private Boolean shouldTriggerAlert(TradeIntensity intensity) {
            Double intensityScore = intensity.getIntensityScore();
            return intensityScore >= 150 || intensityScore <= 50;
        }
    }

    /**
     * TradeIntensity 객체를 JSON 문자열로 변환하는 Mapper
     */
    public static class TradeIntensityToJsonMapper implements MapFunction<TradeIntensity, String> {
        @Override
        public String map(TradeIntensity intensity) throws Exception {
            try {
                return objectMapper.writeValueAsString(intensity);
            } catch (Exception e) {
                logger.error("JSON 직렬화 오류: " + intensity, e);
                return "";
            }
        }
    }
}
