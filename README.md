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

#### 5-1. 기본 실행 (체결강도 파이프라인 활성화)

```bash
# Spring Boot 애플리케이션 실행
./gradlew bootRun

# 또는 백그라운드 실행
./gradlew bootRun &
```

#### 5-2. 파이프라인 선택

켜진 파이프라인은 모두 한 Flink Job에 붙어 함께 실행된다. Kafka 토픽은 한 번만 읽는다.

```bash
# 고래 감지도 함께 활성화
./gradlew bootRun --args="--jobs.whaleDetection.enabled=true"

# 체결강도 비활성화
./gradlew bootRun --args="--jobs.tradeIntensity.enabled=false"
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

### 0. 파이프라인 실행 제어

`FlinkJobRunner`가 기동 시 `crypto-trades`를 한 번 읽어 파싱·검증하고, 켜진 파이프라인(`FlinkPipeline` 구현체)을 모두 붙여 Flink Job 하나(`crypto-tracker`)로 띄운다. 파싱에 실패한 메시지는 `crypto-trades-dlq`로 간다. 수집 원본 확인은 Kafka UI(http://localhost:8080)에서 한다.

```yaml
jobs:
  tradeIntensity:
    enabled: true # 체결강도 지표 (기본값: true)
  whaleDetection:
    enabled: false # 고래 감지 (기본값: false)
```

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

### 3. 체결강도 지표 (TradeIntensityService)

종목별로 최근 1분·5분·15분 구간의 매수·매도 체결 금액(KRW)을 5초마다 집계해 `crypto-metrics` 토픽에 쓴다. 체결 시각(`trade_timestamp`) 기준 이벤트 시간 윈도우라 재시작 후 Kafka에 쌓인 체결을 다시 읽어도 구간이 밀리지 않는다.

- `intensity`: 매수 금액 ÷ 매도 금액 × 100. 매도가 없으면 9999.99로 막는다
- `buyRatio`: 매수 금액 ÷ 전체 금액 × 100
- `netAmount`: 매수 금액 − 매도 금액 (순매수)

```yaml
trade:
  intensity:
    windows-seconds: 60,300,900 # 집계 구간(초)
    slide-seconds: 5 # 갱신 주기(초)
```

```json
{"code":"KRW-BTC","windowSeconds":300,"windowStart":1718000000000,"windowEnd":1718000300000,"windowEndAt":"2024-06-10T15:05:00+09:00","buyAmount":812345000.0,"sellAmount":650000000.0,"totalAmount":1462345000.0,"netAmount":162345000.0,"buyVolume":8.1,"sellVolume":6.5,"tradeCount":412,"intensity":124.98,"buyRatio":55.55,"lastTradeTimestamp":1718000299870}
```

REST API와 대시보드는 이 토픽을 소비해 제공할 예정이며 아직 없다.

## 🔧 설정

### Job 실행 제어

```yaml
# 파이프라인 실행 플래그. 켜진 파이프라인을 모두 붙여 Flink Job 하나로 띄운다
jobs:
  tradeIntensity:
    enabled: true # 체결강도 지표
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

4. **Flink Job이 뜨지 않는 문제**

   **증상**: 기동 로그에 `켜진 파이프라인이 없어 Flink Job 을 시작하지 않습니다`가 찍힘

   **원인**: `jobs.*.enabled`가 모두 false

   **해결**: 하나 이상 켜고 재시작. Job은 `FlinkJobRunner`가 비동기로 띄우므로 웹 서버를 막지 않는다

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
