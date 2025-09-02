package com.crypto.flink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 암호화폐 실시간 데이터 스트리밍 분석을 위한 Spring Boot + Flink 애플리케이션
 * 
 * 주요 기능:
 * - Kafka에서 실시간 암호화폐 데이터 수신
 * - Flink를 사용한 실시간 스트리밍 처리
 * - 가격 변동 분석 및 트렌드 감지
 * - 분석 결과를 Kafka로 전송
 * - REST API를 통한 실시간 모니터링
 */
@SpringBootApplication
@EnableScheduling
public class CryptoFlinkApplication {

    public static void main(String[] args) {
        SpringApplication.run(CryptoFlinkApplication.class, args);
        System.out.println("🚀 Crypto Flink Spring Application Started!");
        System.out.println("📊 Real-time cryptocurrency streaming analysis is ready!");
    }
}
