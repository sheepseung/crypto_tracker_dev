# 🚀 Crypto Tracker - 실시간 암호화폐 분석 플랫폼

Spring Boot + Apache Flink를 활용한 실시간 암호화폐 데이터 스트리밍 분석 플랫폼입니다.

## 📋 목차

- [기술 스택](#기술-스택)
- [시스템 아키텍처](#시스템-아키텍처)
- [빌드 및 실행](#빌드-및-실행)
- [기능 명세서](#기능-명세서)
- [API 문서](#api-문서)
- [모니터링](#모니터링)
- [기술 구현 가이드](./TECHNICAL_GUIDE.md) 📚

## 🛠 기술 스택

### Backend

- **Java 17**
- **Spring Boot 3.2.0**
- **Apache Flink 1.18.1**
- **Apache Kafka 7.5.3**

### Infrastructure

- **Docker & Docker Compose**
- **Zookeeper**
- **Kafka UI (AKHQ)**

### Data Processing

- **Flink Streaming**
- **Sliding Window Processing**
- **Real-time Analytics**

## 🏗 시스템 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Upbit API     │───▶│   Kafka Topic   │───▶│  Flink Stream   │
│  (WebSocket)    │    │  (crypto-trades)│    │   Processing    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   REST API      │◀───│  Spring Boot    │◀───│  Analysis       │
│   (Port 8082)   │    │   Application   │    │   Results       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 빌드 및 실행

### 1. 사전 요구사항

```bash
# Java 17 설치 확인
java -version

# Docker & Docker Compose 설치 확인
docker --version
docker-compose --version

# Python 3.7+ 설치 확인
python3 --version

# Gradle 설치 확인
./gradlew --version
```

### 2. 인프라 서비스 실행

```bash
# Kafka, Zookeeper, Flink 클러스터 실행
docker compose up -d

# 서비스 상태 확인
docker compose ps

# Kafka 토픽 생성
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic crypto-trades --replication-factor 1 --partitions 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic crypto-analysis --replication-factor 1 --partitions 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic crypto-trades-dlq --replication-factor 1 --partitions 1
```

### 3. 데이터 프로듀서 환경 설정및

```bash
# Python 가상환경 생성 및 활성화
python3 -m venv .venv
source .venv/bin/activate

# Python 의존성 설치
pip install -r requirements.txt

```

### 4. 애플리케이션 빌드

```bash
# 프로젝트 빌드 (테스트 제외)
./gradlew clean build -x test

# 또는 JAR 파일만 빌드
./gradlew jar
```

### 5. 애플리케이션 실행

#### 5-1. 기본 실행 (원본 로그만 활성화)

```bash
# Spring Boot 애플리케이션 실행
./gradlew bootRun

# 또는 백그라운드 실행
./gradlew bootRun &
```

#### 5-2. 특정 분석 Job 활성화

```bash
# 체결강도 분석만 활성화
./gradlew bootRun --args="--jobs.rawLogger.enabled=false --jobs.tradeIntensity.enabled=true"

# 고래 감지만 활성화
./gradlew bootRun --args="--jobs.rawLogger.enabled=false --jobs.whaleDetection.enabled=true"

# 모든 분석 Job 활성화
./gradlew bootRun --args="--jobs.rawLogger.enabled=false --jobs.tradeIntensity.enabled=true --jobs.whaleDetection.enabled=true --jobs.cryptoStreaming.enabled=true"
```

#### 5-3. 실행 문제 해결

**문제**: Flink Job이 Spring Boot 웹 서버를 블로킹하는 경우
**해결**: `CompletableFuture.runAsync()`를 사용하여 별도 스레드에서 Flink Job 실행

```bash
# 애플리케이션 재시작
pkill -f "CryptoFlinkApplication"
./gradlew bootRun
```

### 6. 데이터 프로듀서 실행

```bash
# 가상환경 활성화 (필요시)
source .venv/bin/activate

# Upbit WebSocket 체결(trade) 프로듀서 실행 (기본 종목: KRW-BTC, KRW-ETH, KRW-DOGE)
python scripts/producer.py

# 종목·Kafka 주소·토픽은 환경 변수로 바꾼다
UPBIT_MARKETS=KRW-BTC,KRW-XRP KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC=crypto-trades python scripts/producer.py

# 또는 백그라운드 실행
python scripts/producer.py &
```

파싱에 실패한 메시지는 Flink Job이 `crypto-trades-dlq` 토픽으로 보낸다. Kafka UI에서 이 토픽이 비어 있으면 수집 형식이 맞는 것이다.

### 7. 동작 확인

```bash
# 애플리케이션 헬스 체크
curl -s http://localhost:8082/actuator/health

# Swagger UI 접속 확인
curl -s http://localhost:8082/swagger-ui/index.html | head -5

# 입력 토픽 메시지 확인 (수집 데이터)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto-trades --from-beginning --timeout-ms 5000 --max-messages 5

# 출력 토픽 메시지 확인 (분석 결과)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto-analysis --from-beginning --timeout-ms 5000 --max-messages 5

# 프로세스 확인
ps aux | grep "CryptoFlinkApplication" | grep -v grep  # Spring Boot 앱 확인
pgrep -fl producer  # 프로듀서 확인
```

### 8. 서비스 접속

- **Spring Boot 애플리케이션**: http://localhost:8082
- **Swagger UI**: http://localhost:8082/swagger-ui/index.html
- **API Documentation**: http://localhost:8082/v3/api-docs
- **Kafka UI**: http://localhost:8080
- **Flink Web UI**: http://localhost:8081
- **헬스 체크**: http://localhost:8082/actuator/health

### 9. 프로세스 종료

```bash
# Spring Boot 애플리케이션 종료
pkill -f "CryptoFlinkApplication"
# 또는
ps aux | grep "CryptoFlinkApplication" | grep -v grep | awk '{print $2}' | xargs kill -9

# 프로듀서 종료
pkill -f "scripts/producer.py"

# Docker 서비스 종료
docker compose down
```

## 📊 기능 명세서

### 0. Job 실행 제어 및 원본 로그 스트리밍

#### Job 실행 제어 기능

모든 Flink Job은 `application.yml`의 `jobs.*` 플래그로 개별 제어가 가능합니다.

```yaml
jobs:
  rawLogger:
    enabled: true # 원본 로그 스트리밍 (기본값: true)
  cryptoStreaming:
    enabled: false # 가격 분석 (기본값: false)
  tradeIntensity:
    enabled: false # 체결강도 분석 (기본값: false)
  whaleDetection:
    enabled: false # 고래 감지 (기본값: false)
```

#### 원본 로그 스트리밍 (RawLogStreamingService)

- **기능**: Kafka에서 수집한 원본 메시지를 콘솔에 로그로 출력
- **용도**: 데이터 수집 확인, 디버깅, 모니터링
- **로그 형식**: `📥 RAW: {"type":"trade","code":"KRW-BTC","ask_bid":"BID",...}`
- **파이프라인**: Kafka Source → Map(로깅) → 종료 (싱크 없음)

### 1. 실시간 암호화폐 분석 (CryptoAnalysisController)

#### 주요 기능

- **실시간 가격 모니터링**: Upbit WebSocket을 통한 실시간 BTC 가격 데이터 수집
- **가격 변동 분석**: 실시간 가격 변화율 및 변동성 계산
- **트렌드 감지**: 가격 움직임 패턴 분석 및 트렌드 방향 예측
- **상태 모니터링**: Flink Job 상태 및 애플리케이션 헬스 체크

#### API 엔드포인트

```
GET /api/crypto/health              # 서비스 상태 확인
GET /api/crypto/job/status          # Flink Job 상태 확인
GET /api/crypto/analysis/{code}     # 특정 암호화폐 분석 결과
GET /api/crypto/analysis/stats      # 전체 분석 통계
POST /api/crypto/job/restart        # Job 재시작 (관리자용)
```

### 2. 고래 움직임 감지 (WhaleDetectionController)

#### 주요 기능

- **대량 거래 감지**: 슬라이딩 윈도우 기반 거래량 분석
- **매수/매도 패턴 분석**: 고래들의 매수/매도 행동 패턴 감지
- **알림 시스템**: 거래량 임계치별 자동 알림 (CRITICAL, HIGH, MEDIUM, LOW)
- **패턴 분류**: 대량 움직임, 중요 움직임, 보통 움직임 패턴 분류

#### 설정 파라미터

```yaml
whale:
  detection:
    threshold: 0.5 # 고래 움직임 감지 임계치 (BTC)
    window:
      size: 5 # 슬라이딩 윈도우 크기 (초)
      slide: 1 # 슬라이딩 윈도우 슬라이드 간격 (초)
    alert:
      critical: 2.0 # CRITICAL 알림 임계치 (BTC)
      high: 1.0 # HIGH 알림 임계치 (BTC)
      medium: 0.5 # MEDIUM 알림 임계치 (BTC)
```

#### API 엔드포인트

```
GET /api/whale/health                    # 서비스 상태 확인
GET /api/whale/recent?limit=10          # 최근 고래 움직임 조회
GET /api/whale/{code}?limit=20          # 특정 암호화폐 고래 움직임
GET /api/whale/stats                    # 고래 움직임 통계
GET /api/whale/alerts/{level}?limit=10  # 알림 레벨별 필터링
```

### 3. 체결강도 분석 (TradeIntensityController)

#### 주요 기능

- **실시간 체결강도 계산**: 매수/매도 비율 기반 체결강도 점수 산출
- **시장 심리 분석**: BULLISH, BEARISH, NEUTRAL 시장 심리 판단
- **트렌드 방향 예측**: RISING, FALLING, STABLE 트렌드 방향 분석
- **변동성 모니터링**: 체결강도 변화를 통한 시장 변동성 측정

#### 설정 파라미터

```yaml
trade:
  intensity:
    window:
      size: 60 # 슬라이딩 윈도우 크기 (초) - 1분
      slide: 5 # 슬라이딩 윈도우 슬라이드 간격 (초) - 5초마다 이동
    alert:
      bullish: 150 # 강한 매수세 알림 임계치
      bearish: 70 # 강한 매도세 알림 임계치
      volatility: 50 # 변동성 알림 임계치
    thresholds:
      very-high: 150 # 매우 높은 체결강도
      high: 120 # 높은 체결강도
      medium: 90 # 보통 체결강도
      low: 80 # 낮은 체결강도
      very-low: 60 # 매우 낮은 체결강도
```

#### API 엔드포인트

```
GET /api/trade-intensity/health                    # 서비스 상태 확인
GET /api/trade-intensity/recent?limit=10          # 최근 체결강도 조회
GET /api/trade-intensity/{code}?limit=20          # 특정 암호화폐 체결강도
GET /api/trade-intensity/stats                    # 체결강도 통계
GET /api/trade-intensity/sentiment/{sentiment}    # 시장 심리별 필터링
GET /api/trade-intensity/monitor                  # 실시간 모니터링
```

## 🔧 설정

### Job 실행 제어

```yaml
# Job 실행 플래그
jobs:
  rawLogger:
    enabled: true # 원본 로그 스트리밍
  cryptoStreaming:
    enabled: false # 가격 분석
  tradeIntensity:
    enabled: false # 체결강도 분석
  whaleDetection:
    enabled: false # 고래 감지
```

### Flink 설정

```yaml
flink:
  checkpoint:
    interval: 10000 # 체크포인트 간격 (ms)
    timeout: 60000 # 체크포인트 타임아웃 (ms)
    min-pause: 5000 # 최소 체크포인트 간격 (ms)
    max-concurrent: 1 # 동시 체크포인트 수
  parallelism: 1 # 병렬 처리 수
```

### Kafka 설정

```yaml
kafka:
  bootstrap-servers: localhost:9092
  input-topic: crypto-trades
  output-topic: crypto-analysis
  group-id: crypto-flink-group
```

## 📈 모니터링

### 1. 애플리케이션 모니터링

- **Spring Boot Actuator**: http://localhost:8082/actuator
- **Health Check**: http://localhost:8082/actuator/health
- **Metrics**: http://localhost:8082/actuator/metrics

### 2. Flink 모니터링

- **Flink Web UI**: http://localhost:8081
- **Job Manager**: http://localhost:8081/#/job-manager
- **Task Manager**: http://localhost:8081/#/task-manager

### 3. Kafka 모니터링

- **Kafka UI**: http://localhost:8080
- **Topic 관리**: http://localhost:8080/topics
- **Consumer Group**: http://localhost:8080/consumer-groups

## 🐛 문제 해결

### 일반적인 문제들

1. **포트 충돌**

   ```bash
   # 사용 중인 포트 확인
   lsof -i :8082
   lsof -i :9092
   lsof -i :8081
   ```

2. **Docker 컨테이너 문제**

   ```bash
   # 컨테이너 로그 확인
   docker-compose logs kafka
   docker-compose logs flink-jobmanager
   docker-compose logs flink-taskmanager
   ```

3. **Flink Job 실패**

   ```bash
   # Flink Web UI에서 Job 상태 확인
   # http://localhost:8081/#/jobs
   ```

4. **Spring Boot 웹 서버가 시작되지 않는 문제**

   **증상**: Flink Job은 실행되지만 8082 포트에 웹 서버가 바인딩되지 않음

   **원인**: Flink Job의 `env.execute()`가 메인 스레드를 블로킹

   **해결**: `CompletableFuture.runAsync()`를 사용하여 별도 스레드에서 Flink Job 실행

   ```bash
   # 애플리케이션 재시작
   pkill -f "CryptoFlinkApplication"
   ./gradlew bootRun
   ```

5. **Swagger UI 접속 불가**

   **증상**: Swagger UI 페이지가 로드되지 않음

   **원인**: Spring Boot 웹 서버가 완전히 시작되지 않음

   **해결**: 위의 Flink Job 블로킹 문제 해결 후 재시작

## 📝 개발 가이드

### 새로운 분석 기능 추가

1. **DTO 클래스 생성**

   ```java
   @Data
   public class NewAnalysis {
       private String timestamp;
       private String code;
       private double value;
       // ... 기타 필드
   }
   ```

2. **서비스 클래스 구현**

   ```java
   @Service
   public class NewAnalysisService {
       // 분석 로직 구현
   }
   ```

3. **컨트롤러 추가**
   ```java
   @RestController
   @RequestMapping("/api/new-analysis")
   public class NewAnalysisController {
       // API 엔드포인트 구현
   }
   ```

### 설정 변경

`src/main/resources/application.yml` 파일에서 각종 설정을 변경할 수 있습니다.

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

---
