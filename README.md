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
┌─────────────────┐    ┌─────────────────┐    ┌──────────────────────────┐
│  Upbit WebSocket│───▶│  Kafka Topic    │───▶│ Flink Job (Spring 내장)   │
│  trade · 3종목   │    │  crypto-trades  │    │ 파싱 → 체결강도 / 감지     │
└─────────────────┘    └─────────────────┘    └──────────────────────────┘
                                                 │              │
                                                 ▼              ▼
                                        crypto-metrics    crypto-events
                                        (체결강도 지표)     (대량 체결·급등락)
                                                 │              │
                                                 ▼              ▼
                                        REST API · 대시보드 (예정, 저장소 소비)
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
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic crypto-metrics --replication-factor 1 --partitions 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic crypto-events --replication-factor 1 --partitions 1
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
# 감지 이벤트만 끄기
./gradlew bootRun --args="--jobs.detection.enabled=false"

# 체결강도만 끄기
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

# 출력 토픽 메시지 확인 (체결강도 지표, 감지 이벤트)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto-metrics --from-beginning --timeout-ms 5000 --max-messages 5
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto-events --from-beginning --timeout-ms 5000 --max-messages 5

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
  detection:
    enabled: true # 대량 체결·급등락 감지 (기본값: true)
```

### 1. 체결강도 지표 (TradeIntensityService)

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

### 2. 감지 이벤트 (DetectionService)

대량 체결(F2)과 급등·급락(F3)을 체결마다 즉시 판정해 `crypto-events` 토픽에 쓴다. 시간은 모두 체결 시각 기준이고 금액은 원화다.

| type | 조건 | 재감지 억제 |
| --- | --- | --- |
| `LARGE_TRADE` | 한 건 체결 금액 ≥ 3억 원 | 같은 종목·방향 60초 |
| `LARGE_TRADE_BURST` | 10초 안에 같은 방향 체결 합 ≥ 3억 원 (단일 조건에 걸린 체결은 제외) | 같은 종목·방향 60초 |
| `PRICE_SURGE` / `PRICE_DROP` | 5분 구간의 가장 오래된 체결가 대비 ±2% | 같은 종목·방향 5분. 직전 이벤트 가격에서 2% 더 움직이면 그 전에도 낸다 |

```yaml
detection:
  large-trade:
    single-amount: 300000000
    burst-window-seconds: 10
    burst-amount: 300000000
    cooldown-seconds: 60
  price-move:
    window-seconds: 300
    threshold-percent: 2.0
    cooldown-seconds: 300
```

```json
{"type":"LARGE_TRADE","code":"KRW-BTC","eventTime":1718000123456,"eventTimeAt":"2024-06-10T15:02:03.456+09:00","side":"BID","amount":350000000.0,"price":100000000.0,"tradeCount":1,"message":"KRW-BTC 매수 3.50억 원 단일 체결 @ 1.00억 원"}
{"type":"PRICE_SURGE","code":"KRW-DOGE","eventTime":1718000200000,"eventTimeAt":"2024-06-10T15:03:20+09:00","price":204.1,"priceFrom":200.0,"priceTo":204.1,"changePercent":2.05,"windowSeconds":300,"message":"KRW-DOGE 5분 내 +2.05% (200원 → 204원)"}
```

REST API와 대시보드는 두 토픽을 소비해 제공할 예정이며 아직 없다. 메신저 전송은 범위 밖이다.

## 🔧 설정

### Job 실행 제어

```yaml
# 파이프라인 실행 플래그. 켜진 파이프라인을 모두 붙여 Flink Job 하나로 띄운다
jobs:
  tradeIntensity:
    enabled: true # 체결강도 지표
  detection:
    enabled: true # 대량 체결·급등락 감지
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
  input-topic: crypto-trades # 수집 원본 (producer.py)
  dead-letter-topic: crypto-trades-dlq # 파싱 실패 원본
  metrics-topic: crypto-metrics # 체결강도 지표
  events-topic: crypto-events # 감지 이벤트
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
