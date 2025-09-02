package com.crypto.flink.controller;

import com.crypto.flink.dto.WhaleMovement;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 고래 움직임 감지 결과를 조회하는 REST API 컨트롤러
 * 
 * 주요 기능:
 * - 실시간 고래 움직임 감지 결과 조회
 * - 특정 암호화폐의 고래 움직임 이력 조회
 * - 고래 움직임 통계 및 패턴 분석 결과 조회
 * - 알림 레벨별 고래 움직임 필터링
 */
@Tag(name = "Whale Detection", description = "고래 움직임 감지 API")
@RestController
@RequestMapping("/api/whale")
@CrossOrigin(origins = "*")
public class WhaleDetectionController {

    /**
     * 고래 감지 서비스 상태 확인
     */
    @Operation(summary = "고래 감지 서비스 상태 확인", description = "고래 움직임 감지 서비스의 현재 상태를 확인합니다.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "서비스가 정상적으로 작동 중"),
            @ApiResponse(responseCode = "503", description = "서비스 이용 불가")
    })
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Whale Detection Service");
        response.put("timestamp", System.currentTimeMillis());
        response.put("message", "고래 움직임 감지 서비스가 정상적으로 실행 중입니다.");
        response.put("features", Arrays.asList(
                "실시간 고래 움직임 감지",
                "슬라이딩 윈도우 기반 거래량 분석",
                "매수/매도 패턴 감지",
                "이상 거래량 알림"));

        return ResponseEntity.ok(response);
    }

    /**
     * 최근 고래 움직임 감지 결과 조회 (시뮬레이션)
     */
    @GetMapping("/recent")
    public ResponseEntity<List<WhaleMovement>> getRecentWhaleMovements(
            @RequestParam(defaultValue = "10") int limit) {

        List<WhaleMovement> movements = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < limit; i++) {
            // 랜덤 고래 움직임 데이터 생성
            String[] codes = { "BTC", "ETH", "ADA", "DOT", "LINK" };
            String code = codes[random.nextInt(codes.length)];

            // 랜덤 거래량 생성
            double buyVolume = random.nextDouble() * 5.0;
            double sellVolume = random.nextDouble() * 5.0;
            double netVolume = buyVolume - sellVolume;

            // 움직임 유형 결정
            String movementType;
            if (Math.abs(netVolume) > 2.0) {
                movementType = netVolume > 0 ? "AGGRESSIVE_BUY" : "AGGRESSIVE_SELL";
            } else if (Math.abs(netVolume) > 0.5) {
                movementType = "MODERATE_MOVEMENT";
            } else {
                movementType = "NORMAL";
            }

            // 알림 레벨 결정
            String alertLevel;
            if (Math.abs(netVolume) > 3.0) {
                alertLevel = "CRITICAL";
            } else if (Math.abs(netVolume) > 1.5) {
                alertLevel = "HIGH";
            } else if (Math.abs(netVolume) > 0.5) {
                alertLevel = "MEDIUM";
            } else {
                alertLevel = "LOW";
            }

            WhaleMovement movement = new WhaleMovement(
                    LocalDateTime.now().minusMinutes(i).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    code,
                    LocalDateTime.now().minusMinutes(i + 1).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    LocalDateTime.now().minusMinutes(i).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    5,
                    buyVolume,
                    sellVolume,
                    netVolume,
                    buyVolume > 0 ? sellVolume / buyVolume : 0.0,
                    0.5,
                    Math.abs(netVolume) > 0.5,
                    movementType,
                    Math.min(Math.abs(netVolume) * 20, 100.0),
                    detectPattern(buyVolume, sellVolume, netVolume),
                    alertLevel);

            movements.add(movement);
        }

        return ResponseEntity.ok(movements);
    }

    /**
     * 특정 암호화폐의 고래 움직임 조회 (시뮬레이션)
     */
    @GetMapping("/{code}")
    public ResponseEntity<List<WhaleMovement>> getWhaleMovementsByCode(
            @PathVariable String code,
            @RequestParam(defaultValue = "20") int limit) {

        List<WhaleMovement> movements = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < limit; i++) {
            // 랜덤 거래량 생성
            double buyVolume = random.nextDouble() * 3.0;
            double sellVolume = random.nextDouble() * 3.0;
            double netVolume = buyVolume - sellVolume;

            // 움직임 유형 결정
            String movementType;
            if (Math.abs(netVolume) > 1.5) {
                movementType = netVolume > 0 ? "AGGRESSIVE_BUY" : "AGGRESSIVE_SELL";
            } else if (Math.abs(netVolume) > 0.3) {
                movementType = "MODERATE_MOVEMENT";
            } else {
                movementType = "NORMAL";
            }

            // 알림 레벨 결정
            String alertLevel;
            if (Math.abs(netVolume) > 2.0) {
                alertLevel = "CRITICAL";
            } else if (Math.abs(netVolume) > 1.0) {
                alertLevel = "HIGH";
            } else if (Math.abs(netVolume) > 0.3) {
                alertLevel = "MEDIUM";
            } else {
                alertLevel = "LOW";
            }

            WhaleMovement movement = new WhaleMovement(
                    LocalDateTime.now().minusMinutes(i * 5).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    code,
                    LocalDateTime.now().minusMinutes(i * 5 + 1).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    LocalDateTime.now().minusMinutes(i * 5).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    5,
                    buyVolume,
                    sellVolume,
                    netVolume,
                    buyVolume > 0 ? sellVolume / buyVolume : 0.0,
                    0.5,
                    Math.abs(netVolume) > 0.5,
                    movementType,
                    Math.min(Math.abs(netVolume) * 25, 100.0),
                    detectPattern(buyVolume, sellVolume, netVolume),
                    alertLevel);

            movements.add(movement);
        }

        return ResponseEntity.ok(movements);
    }

    /**
     * 고래 움직임 통계 조회 (시뮬레이션)
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getWhaleMovementStats() {
        Map<String, Object> stats = new HashMap<>();
        Random random = new Random();

        // 전체 통계
        stats.put("totalDetections", 1250);
        stats.put("criticalAlerts", 45);
        stats.put("highAlerts", 120);
        stats.put("mediumAlerts", 280);
        stats.put("lowAlerts", 805);

        // 움직임 유형별 통계
        Map<String, Integer> movementTypes = new HashMap<>();
        movementTypes.put("AGGRESSIVE_BUY", 180);
        movementTypes.put("AGGRESSIVE_SELL", 165);
        movementTypes.put("BALANCED", 95);
        movementTypes.put("NORMAL", 810);
        stats.put("movementTypes", movementTypes);

        // 암호화폐별 통계
        Map<String, Integer> cryptoStats = new HashMap<>();
        cryptoStats.put("BTC", 450);
        cryptoStats.put("ETH", 320);
        cryptoStats.put("ADA", 180);
        cryptoStats.put("DOT", 150);
        cryptoStats.put("LINK", 150);
        stats.put("cryptoStats", cryptoStats);

        // 시간대별 통계 (최근 24시간)
        Map<String, Integer> hourlyStats = new HashMap<>();
        for (int i = 0; i < 24; i++) {
            hourlyStats.put(String.valueOf(i), random.nextInt(50) + 10);
        }
        stats.put("hourlyStats", hourlyStats);

        stats.put("lastUpdated", System.currentTimeMillis());

        return ResponseEntity.ok(stats);
    }

    /**
     * 알림 레벨별 고래 움직임 필터링 (시뮬레이션)
     */
    @GetMapping("/alerts/{level}")
    public ResponseEntity<List<WhaleMovement>> getWhaleMovementsByAlertLevel(
            @PathVariable String level,
            @RequestParam(defaultValue = "10") int limit) {

        List<WhaleMovement> movements = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < limit; i++) {
            String[] codes = { "BTC", "ETH", "ADA", "DOT", "LINK" };
            String code = codes[random.nextInt(codes.length)];

            // 알림 레벨에 따른 거래량 생성
            double baseVolume;
            switch (level.toUpperCase()) {
                case "CRITICAL":
                    baseVolume = 2.0 + random.nextDouble() * 3.0; // 2.0 ~ 5.0
                    break;
                case "HIGH":
                    baseVolume = 1.0 + random.nextDouble() * 1.0; // 1.0 ~ 2.0
                    break;
                case "MEDIUM":
                    baseVolume = 0.5 + random.nextDouble() * 0.5; // 0.5 ~ 1.0
                    break;
                default:
                    baseVolume = random.nextDouble() * 0.5; // 0.0 ~ 0.5
            }

            double buyVolume = baseVolume * (0.5 + random.nextDouble() * 0.5);
            double sellVolume = baseVolume * (0.5 + random.nextDouble() * 0.5);
            double netVolume = buyVolume - sellVolume;

            String movementType = netVolume > 0 ? "AGGRESSIVE_BUY" : "AGGRESSIVE_SELL";

            WhaleMovement movement = new WhaleMovement(
                    LocalDateTime.now().minusMinutes(i * 3).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    code,
                    LocalDateTime.now().minusMinutes(i * 3 + 1).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    LocalDateTime.now().minusMinutes(i * 3).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    5,
                    buyVolume,
                    sellVolume,
                    netVolume,
                    buyVolume > 0 ? sellVolume / buyVolume : 0.0,
                    0.5,
                    true,
                    movementType,
                    Math.min(Math.abs(netVolume) * 30, 100.0),
                    detectPattern(buyVolume, sellVolume, netVolume),
                    level.toUpperCase());

            movements.add(movement);
        }

        return ResponseEntity.ok(movements);
    }

    /**
     * 패턴 감지 헬퍼 메서드
     */
    private String detectPattern(double buyVolume, double sellVolume, double netVolume) {
        double totalVolume = buyVolume + sellVolume;

        if (totalVolume > 3.0 && Math.abs(netVolume) > 2.0) {
            return "MASSIVE_MOVEMENT";
        } else if (totalVolume > 1.5 && Math.abs(netVolume) > 1.0) {
            return "SIGNIFICANT_MOVEMENT";
        } else if (Math.abs(netVolume) > 0.5) {
            return "MODERATE_MOVEMENT";
        } else {
            return "NO_PATTERN";
        }
    }
}
