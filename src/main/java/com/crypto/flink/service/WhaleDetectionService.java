package com.crypto.flink.service;

import com.crypto.flink.dto.CryptoTrade;
import com.crypto.flink.dto.WhaleMovement;
import com.crypto.flink.job.FlinkPipeline;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 고래(대량 거래자) 움직임을 실시간으로 감지하는 Flink 스트리밍 서비스
 * 
 * 주요 기능:
 * - 슬라이딩 윈도우를 사용한 실시간 거래량 집계
 * - 매수/매도 체결량 분석을 통한 고래 움직임 감지
 * - 이상 거래량 패턴 감지 및 알림 생성
 * - 복잡한 이벤트 패턴 감지 (CEP)
 */
@Service
public class WhaleDetectionService implements FlinkPipeline {

    private static final Logger logger = LoggerFactory.getLogger(WhaleDetectionService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${whale.detection.threshold:0.5}")
    private Double whaleThreshold;

    @Value("${whale.detection.window.size:5}")
    private Integer windowSizeSeconds;

    @Value("${whale.detection.window.slide:1}")
    private Integer windowSlideSeconds;

    @Value("${jobs.whaleDetection.enabled:false}")
    private boolean jobEnabled;

    @Autowired
    @Qualifier("kafkaSink")
    private KafkaSink<String> kafkaSink;

    @Override
    public String name() {
        return "whaleDetection";
    }

    @Override
    public boolean isEnabled() {
        return jobEnabled;
    }

    @Override
    public void attach(DataStream<CryptoTrade> tradeStream) {
        logger.info("📊 고래 감지 설정값 - 임계치: {} BTC, 윈도우 크기: {}초, 슬라이드: {}초",
                whaleThreshold, windowSizeSeconds, windowSlideSeconds);

        // 슬라이딩 윈도우를 사용한 거래량 집계
        DataStream<WhaleMovement> whaleMovementStream = tradeStream
                .keyBy(CryptoTrade::getCode)
                .window(SlidingProcessingTimeWindows.of(
                        Time.seconds(windowSizeSeconds),
                        Time.seconds(windowSlideSeconds)))
                .aggregate(new WhaleVolumeAggregator())
                .name("Whale Volume Aggregation");

        // 고래 움직임 분석 및 패턴 감지
        DataStream<WhaleMovement> analyzedStream = whaleMovementStream
                .map(new WhaleMovementAnalyzer())
                .name("Whale Movement Analysis");

        // 분석 결과를 JSON 문자열로 변환
        DataStream<String> outputStream = analyzedStream
                .map(new WhaleMovementToJsonMapper())
                .name("WhaleMovement to JSON");

        // Kafka로 결과 전송
        outputStream.sinkTo(kafkaSink).name("Kafka Sink - Whale Detection");
    }

    /**
     * 고래 거래량을 집계하는 AggregateFunction
     */
    public static class WhaleVolumeAggregator
            implements AggregateFunction<CryptoTrade, Tuple3<Double, Double, Integer>, WhaleMovement> {

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
        public WhaleMovement getResult(Tuple3<Double, Double, Integer> accumulator) {
            Double buyVolume = accumulator.f0;
            Double sellVolume = accumulator.f1;
            Integer count = accumulator.f2;

            // 기본값 반환 (실제로는 윈도우 정보가 필요)
            return new WhaleMovement(
                    LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    "UNKNOWN",
                    LocalDateTime.now().minusSeconds(5).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    5,
                    buyVolume,
                    sellVolume,
                    buyVolume - sellVolume,
                    buyVolume > 0 ? sellVolume / buyVolume : 0.0,
                    0.5,
                    false,
                    "NORMAL",
                    0.0,
                    "NO_PATTERN",
                    "LOW");
        }

        @Override
        public Tuple3<Double, Double, Integer> merge(Tuple3<Double, Double, Integer> a,
                Tuple3<Double, Double, Integer> b) {
            return new Tuple3<>(a.f0 + b.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }

    /**
     * 고래 움직임을 분석하는 Mapper
     */
    public static class WhaleMovementAnalyzer extends RichMapFunction<WhaleMovement, WhaleMovement> {

        @Override
        public WhaleMovement map(WhaleMovement movement) throws Exception {

            // 고래 움직임 감지 로직
            Double netVolume = Math.abs(movement.getNetVolume());
            Double totalVolume = movement.getTotalBuyVolume() + movement.getTotalSellVolume();

            // 임계치 기반 고래 움직임 판단
            boolean isWhaleMovement = netVolume > 0.5; // 0.5 BTC 이상

            // 움직임 유형 결정
            String movementType = determineMovementType(movement);

            // 신뢰도 점수 계산
            Double confidenceScore = calculateConfidenceScore(movement);

            // 패턴 감지
            String patternDetected = detectPattern(movement);

            // 알림 레벨 결정
            String alertLevel = determineAlertLevel(movement, isWhaleMovement);

            // 분석 결과 업데이트
            movement.setIsWhaleMovement(isWhaleMovement);
            movement.setMovementType(movementType);
            movement.setConfidenceScore(confidenceScore);
            movement.setPatternDetected(patternDetected);
            movement.setAlertLevel(alertLevel);

            // 로그 출력
            if (isWhaleMovement) {
                logger.info("🐋 고래 움직임 감지: {} - {} - 순거래량: {} BTC - 알림레벨: {}",
                        movement.getCode(), movementType,
                        String.format("%.4f", netVolume), alertLevel);
            }

            return movement;
        }

        /**
         * 움직임 유형 결정
         */
        private String determineMovementType(WhaleMovement movement) {
            Double netVolume = movement.getNetVolume();
            Double buyVolume = movement.getTotalBuyVolume();
            Double sellVolume = movement.getTotalSellVolume();

            if (Math.abs(netVolume) < 0.1) {
                return "BALANCED";
            } else if (netVolume > 0.5) {
                return "AGGRESSIVE_BUY";
            } else if (netVolume < -0.5) {
                return "AGGRESSIVE_SELL";
            } else {
                return "NORMAL";
            }
        }

        /**
         * 신뢰도 점수 계산
         */
        private Double calculateConfidenceScore(WhaleMovement movement) {
            Double totalVolume = movement.getTotalBuyVolume() + movement.getTotalSellVolume();
            Double netVolume = Math.abs(movement.getNetVolume());

            if (totalVolume == 0)
                return 0.0;

            // 거래량 대비 순거래량 비율로 신뢰도 계산
            Double volumeRatio = netVolume / totalVolume;
            return Math.min(volumeRatio * 100, 100.0);
        }

        /**
         * 패턴 감지
         */
        private String detectPattern(WhaleMovement movement) {
            Double netVolume = movement.getNetVolume();
            Double totalVolume = movement.getTotalBuyVolume() + movement.getTotalSellVolume();

            if (totalVolume > 2.0 && Math.abs(netVolume) > 1.0) {
                return "MASSIVE_MOVEMENT";
            } else if (totalVolume > 1.0 && Math.abs(netVolume) > 0.5) {
                return "SIGNIFICANT_MOVEMENT";
            } else if (Math.abs(netVolume) > 0.3) {
                return "MODERATE_MOVEMENT";
            } else {
                return "NO_PATTERN";
            }
        }

        /**
         * 알림 레벨 결정
         */
        private String determineAlertLevel(WhaleMovement movement, boolean isWhaleMovement) {
            if (!isWhaleMovement)
                return "LOW";

            Double netVolume = Math.abs(movement.getNetVolume());
            Double totalVolume = movement.getTotalBuyVolume() + movement.getTotalSellVolume();

            if (netVolume > 2.0 || totalVolume > 5.0) {
                return "CRITICAL";
            } else if (netVolume > 1.0 || totalVolume > 2.0) {
                return "HIGH";
            } else if (netVolume > 0.5 || totalVolume > 1.0) {
                return "MEDIUM";
            } else {
                return "LOW";
            }
        }
    }

    /**
     * WhaleMovement 객체를 JSON 문자열로 변환하는 Mapper
     */
    public static class WhaleMovementToJsonMapper implements MapFunction<WhaleMovement, String> {
        @Override
        public String map(WhaleMovement movement) throws Exception {
            try {
                return objectMapper.writeValueAsString(movement);
            } catch (Exception e) {
                logger.error("JSON 직렬화 오류: " + movement, e);
                return "";
            }
        }
    }
}
