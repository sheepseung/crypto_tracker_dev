package com.crypto.flink.controller;

import com.crypto.flink.dto.TradeIntensity;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 실시간 체결강도(Trade Intensity) 데이터를 조회하는 REST API 컨트롤러
 * 
 * 주요 기능:
 * - 실시간 체결강도 데이터 조회
 * - 특정 암호화폐의 체결강도 이력 조회
 * - 체결강도 통계 및 트렌드 분석 결과 조회
 * - 시장 심리별 체결강도 필터링
 * - WebSocket을 통한 실시간 스트리밍 (향후 구현)
 */
@Tag(name = "Trade Intensity", description = "실시간 체결강도 분석 API")
@RestController
@RequestMapping("/api/trade-intensity")
@CrossOrigin(origins = "*")
public class TradeIntensityController {

    /**
     * 체결강도 서비스 상태 확인
     */
    @Operation(summary = "체결강도 서비스 상태 확인", description = "체결강도 분석 서비스의 현재 상태를 확인합니다.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "서비스가 정상적으로 작동 중"),
            @ApiResponse(responseCode = "503", description = "서비스 이용 불가")
    })
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Trade Intensity Service");
        response.put("timestamp", System.currentTimeMillis());
        response.put("message", "실시간 체결강도 계산 서비스가 정상적으로 실행 중입니다.");
        response.put("features", Arrays.asList(
                "실시간 체결강도 계산",
                "슬라이딩 윈도우 기반 체결량 분석",
                "시장 심리 및 트렌드 분석",
                "체결강도 변화 알림",
                "WebSocket 실시간 스트리밍"));

        return ResponseEntity.ok(response);
    }

    /**
     * 최근 체결강도 데이터 조회 (시뮬레이션)
     */
    @GetMapping("/recent")
    public ResponseEntity<List<TradeIntensity>> getRecentTradeIntensity(
            @RequestParam(defaultValue = "10") int limit) {

        List<TradeIntensity> intensities = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < limit; i++) {
            // 랜덤 체결강도 데이터 생성
            String[] codes = { "BTC", "ETH", "ADA", "DOT", "LINK" };
            String code = codes[random.nextInt(codes.length)];

            // 랜덤 체결량 생성
            double buyVolume = random.nextDouble() * 5.0;
            double sellVolume = random.nextDouble() * 5.0;
            double totalVolume = buyVolume + sellVolume;
            double intensityScore = sellVolume > 0 ? (buyVolume / sellVolume) * 100 : 100.0;

            // 시장 심리 결정
            String marketSentiment;
            if (intensityScore >= 120)
                marketSentiment = "BULLISH";
            else if (intensityScore <= 80)
                marketSentiment = "BEARISH";
            else
                marketSentiment = "NEUTRAL";

            // 강도 레벨 결정
            String intensityLevel;
            if (intensityScore >= 150)
                intensityLevel = "VERY_HIGH";
            else if (intensityScore >= 120)
                intensityLevel = "HIGH";
            else if (intensityScore >= 90)
                intensityLevel = "MEDIUM";
            else if (intensityScore >= 60)
                intensityLevel = "LOW";
            else
                intensityLevel = "VERY_LOW";

            // 트렌드 방향 결정
            String trendDirection;
            if (intensityScore > 110)
                trendDirection = "RISING";
            else if (intensityScore < 90)
                trendDirection = "FALLING";
            else
                trendDirection = "STABLE";

            // 알림 레벨 결정
            String alertLevel;
            if (intensityScore >= 200 || intensityScore <= 30)
                alertLevel = "CRITICAL";
            else if (intensityScore >= 150 || intensityScore <= 50)
                alertLevel = "HIGH";
            else if (intensityScore >= 120 || intensityScore <= 80)
                alertLevel = "MEDIUM";
            else
                alertLevel = "LOW";

            // 알림 메시지 생성
            String alertMessage;
            if (intensityScore >= 150)
                alertMessage = "매우 강한 매수세가 관찰되고 있습니다!";
            else if (intensityScore <= 50)
                alertMessage = "매우 강한 매도세가 관찰되고 있습니다!";
            else if (intensityScore >= 120)
                alertMessage = "강한 매수세가 지속되고 있습니다.";
            else if (intensityScore <= 80)
                alertMessage = "강한 매도세가 지속되고 있습니다.";
            else
                alertMessage = "시장이 안정적으로 유지되고 있습니다.";

            TradeIntensity intensity = new TradeIntensity(
                    LocalDateTime.now().minusMinutes(i).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    code,
                    LocalDateTime.now().minusMinutes(i + 1).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    LocalDateTime.now().minusMinutes(i).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    60,
                    buyVolume,
                    sellVolume,
                    totalVolume,
                    intensityScore,
                    intensityScore,
                    marketSentiment,
                    intensityLevel,
                    trendDirection,
                    Math.min(Math.abs(intensityScore - 100) * 2, 100.0),
                    alertLevel,
                    alertMessage,
                    intensityScore >= 150 || intensityScore <= 50);

            intensities.add(intensity);
        }

        return ResponseEntity.ok(intensities);
    }

    /**
     * 특정 암호화폐의 체결강도 조회 (시뮬레이션)
     */
    @GetMapping("/{code}")
    public ResponseEntity<List<TradeIntensity>> getTradeIntensityByCode(
            @PathVariable String code,
            @RequestParam(defaultValue = "20") int limit) {

        List<TradeIntensity> intensities = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < limit; i++) {
            // 랜덤 체결량 생성
            double buyVolume = random.nextDouble() * 3.0;
            double sellVolume = random.nextDouble() * 3.0;
            double totalVolume = buyVolume + sellVolume;
            double intensityScore = sellVolume > 0 ? (buyVolume / sellVolume) * 100 : 100.0;

            // 시장 심리 결정
            String marketSentiment;
            if (intensityScore >= 120)
                marketSentiment = "BULLISH";
            else if (intensityScore <= 80)
                marketSentiment = "BEARISH";
            else
                marketSentiment = "NEUTRAL";

            // 강도 레벨 결정
            String intensityLevel;
            if (intensityScore >= 150)
                intensityLevel = "VERY_HIGH";
            else if (intensityScore >= 120)
                intensityLevel = "HIGH";
            else if (intensityScore >= 90)
                intensityLevel = "MEDIUM";
            else if (intensityScore >= 60)
                intensityLevel = "LOW";
            else
                intensityLevel = "VERY_LOW";

            // 트렌드 방향 결정
            String trendDirection;
            if (intensityScore > 110)
                trendDirection = "RISING";
            else if (intensityScore < 90)
                trendDirection = "FALLING";
            else
                trendDirection = "STABLE";

            // 알림 레벨 결정
            String alertLevel;
            if (intensityScore >= 200 || intensityScore <= 30)
                alertLevel = "CRITICAL";
            else if (intensityScore >= 150 || intensityScore <= 50)
                alertLevel = "HIGH";
            else if (intensityScore >= 120 || intensityScore <= 80)
                alertLevel = "MEDIUM";
            else
                alertLevel = "LOW";

            // 알림 메시지 생성
            String alertMessage;
            if (intensityScore >= 150)
                alertMessage = "매우 강한 매수세가 관찰되고 있습니다!";
            else if (intensityScore <= 50)
                alertMessage = "매우 강한 매도세가 관찰되고 있습니다!";
            else if (intensityScore >= 120)
                alertMessage = "강한 매수세가 지속되고 있습니다.";
            else if (intensityScore <= 80)
                alertMessage = "강한 매도세가 지속되고 있습니다.";
            else
                alertMessage = "시장이 안정적으로 유지되고 있습니다.";

            TradeIntensity intensity = new TradeIntensity(
                    LocalDateTime.now().minusMinutes(i * 5).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    code,
                    LocalDateTime.now().minusMinutes(i * 5 + 1).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    LocalDateTime.now().minusMinutes(i * 5).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    60,
                    buyVolume,
                    sellVolume,
                    totalVolume,
                    intensityScore,
                    intensityScore,
                    marketSentiment,
                    intensityLevel,
                    trendDirection,
                    Math.min(Math.abs(intensityScore - 100) * 2, 100.0),
                    alertLevel,
                    alertMessage,
                    intensityScore >= 150 || intensityScore <= 50);

            intensities.add(intensity);
        }

        return ResponseEntity.ok(intensities);
    }

    /**
     * 체결강도 통계 조회 (시뮬레이션)
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getTradeIntensityStats() {
        Map<String, Object> stats = new HashMap<>();
        Random random = new Random();

        // 전체 통계
        stats.put("totalCalculations", 1800);
        stats.put("criticalAlerts", 35);
        stats.put("highAlerts", 95);
        stats.put("mediumAlerts", 220);
        stats.put("lowAlerts", 1450);

        // 시장 심리별 통계
        Map<String, Integer> sentimentStats = new HashMap<>();
        sentimentStats.put("BULLISH", 420);
        sentimentStats.put("BEARISH", 380);
        sentimentStats.put("NEUTRAL", 1000);
        stats.put("sentimentStats", sentimentStats);

        // 강도 레벨별 통계
        Map<String, Integer> levelStats = new HashMap<>();
        levelStats.put("VERY_HIGH", 85);
        levelStats.put("HIGH", 335);
        levelStats.put("MEDIUM", 1000);
        levelStats.put("LOW", 320);
        levelStats.put("VERY_LOW", 60);
        stats.put("levelStats", levelStats);

        // 암호화폐별 통계
        Map<String, Integer> cryptoStats = new HashMap<>();
        cryptoStats.put("BTC", 600);
        cryptoStats.put("ETH", 450);
        cryptoStats.put("ADA", 300);
        cryptoStats.put("DOT", 250);
        cryptoStats.put("LINK", 200);
        stats.put("cryptoStats", cryptoStats);

        // 시간대별 통계 (최근 24시간)
        Map<String, Integer> hourlyStats = new HashMap<>();
        for (int i = 0; i < 24; i++) {
            hourlyStats.put(String.valueOf(i), random.nextInt(80) + 20);
        }
        stats.put("hourlyStats", hourlyStats);

        stats.put("lastUpdated", System.currentTimeMillis());

        return ResponseEntity.ok(stats);
    }

    /**
     * 시장 심리별 체결강도 필터링 (시뮬레이션)
     */
    @GetMapping("/sentiment/{sentiment}")
    public ResponseEntity<List<TradeIntensity>> getTradeIntensityBySentiment(
            @PathVariable String sentiment,
            @RequestParam(defaultValue = "10") int limit) {

        List<TradeIntensity> intensities = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < limit; i++) {
            String[] codes = { "BTC", "ETH", "ADA", "DOT", "LINK" };
            String code = codes[random.nextInt(codes.length)];

            // 시장 심리에 따른 체결강도 생성
            double intensityScore;
            switch (sentiment.toUpperCase()) {
                case "BULLISH":
                    intensityScore = 120.0 + random.nextDouble() * 80.0; // 120 ~ 200
                    break;
                case "BEARISH":
                    intensityScore = random.nextDouble() * 80.0; // 0 ~ 80
                    break;
                default: // NEUTRAL
                    intensityScore = 80.0 + random.nextDouble() * 40.0; // 80 ~ 120
            }

            double buyVolume = random.nextDouble() * 3.0;
            double sellVolume = random.nextDouble() * 3.0;
            double totalVolume = buyVolume + sellVolume;

            // 강도 레벨 결정
            String intensityLevel;
            if (intensityScore >= 150)
                intensityLevel = "VERY_HIGH";
            else if (intensityScore >= 120)
                intensityLevel = "HIGH";
            else if (intensityScore >= 90)
                intensityLevel = "MEDIUM";
            else if (intensityScore >= 60)
                intensityLevel = "LOW";
            else
                intensityLevel = "VERY_LOW";

            // 트렌드 방향 결정
            String trendDirection;
            if (intensityScore > 110)
                trendDirection = "RISING";
            else if (intensityScore < 90)
                trendDirection = "FALLING";
            else
                trendDirection = "STABLE";

            // 알림 레벨 결정
            String alertLevel;
            if (intensityScore >= 200 || intensityScore <= 30)
                alertLevel = "CRITICAL";
            else if (intensityScore >= 150 || intensityScore <= 50)
                alertLevel = "HIGH";
            else if (intensityScore >= 120 || intensityScore <= 80)
                alertLevel = "MEDIUM";
            else
                alertLevel = "LOW";

            // 알림 메시지 생성
            String alertMessage;
            if (intensityScore >= 150)
                alertMessage = "매우 강한 매수세가 관찰되고 있습니다!";
            else if (intensityScore <= 50)
                alertMessage = "매우 강한 매도세가 관찰되고 있습니다!";
            else if (intensityScore >= 120)
                alertMessage = "강한 매수세가 지속되고 있습니다.";
            else if (intensityScore <= 80)
                alertMessage = "강한 매도세가 지속되고 있습니다.";
            else
                alertMessage = "시장이 안정적으로 유지되고 있습니다.";

            TradeIntensity intensity = new TradeIntensity(
                    LocalDateTime.now().minusMinutes(i * 3).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    code,
                    LocalDateTime.now().minusMinutes(i * 3 + 1).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    LocalDateTime.now().minusMinutes(i * 3).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    60,
                    buyVolume,
                    sellVolume,
                    totalVolume,
                    intensityScore,
                    intensityScore,
                    sentiment.toUpperCase(),
                    intensityLevel,
                    trendDirection,
                    Math.min(Math.abs(intensityScore - 100) * 2, 100.0),
                    alertLevel,
                    alertMessage,
                    intensityScore >= 150 || intensityScore <= 50);

            intensities.add(intensity);
        }

        return ResponseEntity.ok(intensities);
    }

    /**
     * 실시간 체결강도 모니터링 (시뮬레이션)
     */
    @GetMapping("/monitor")
    public ResponseEntity<Map<String, Object>> getRealTimeMonitor() {
        Map<String, Object> monitor = new HashMap<>();
        Random random = new Random();

        // 현재 시장 상황 요약
        Map<String, Object> marketSummary = new HashMap<>();
        marketSummary.put("overallSentiment", "BULLISH");
        marketSummary.put("averageIntensity", 125.6);
        marketSummary.put("activeAlerts", 8);
        marketSummary.put("trendDirection", "RISING");
        marketSummary.put("volatilityLevel", "MEDIUM");

        // 주요 암호화폐별 체결강도
        Map<String, Object> cryptoIntensities = new HashMap<>();
        String[] codes = { "BTC", "ETH", "ADA", "DOT", "LINK" };
        for (String code : codes) {
            Map<String, Object> crypto = new HashMap<>();
            double intensity = 80.0 + random.nextDouble() * 80.0; // 80 ~ 160
            crypto.put("intensityScore", Math.round(intensity * 100.0) / 100.0);
            crypto.put("sentiment", intensity > 120 ? "BULLISH" : intensity < 80 ? "BEARISH" : "NEUTRAL");
            crypto.put("trend", intensity > 110 ? "RISING" : intensity < 90 ? "FALLING" : "STABLE");
            crypto.put("alertLevel", intensity > 150 || intensity < 50 ? "HIGH" : "LOW");
            cryptoIntensities.put(code, crypto);
        }

        monitor.put("timestamp", System.currentTimeMillis());
        monitor.put("marketSummary", marketSummary);
        monitor.put("cryptoIntensities", cryptoIntensities);
        monitor.put("lastUpdate", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        return ResponseEntity.ok(monitor);
    }
}
