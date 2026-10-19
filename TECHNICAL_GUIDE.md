# Crypto Tracker 기술 가이드

이 문서는 코드를 고치거나 기능을 더할 사람을 위한 구조 설명이다. 실행 방법과 API 목록은 `README.md`에 있다.

## 1. 한눈에 보는 구조

```
업비트 WebSocket ──producer.py──▶ Kafka crypto-trades
      (trade, 3종목)                     │
                                         ▼  FlinkJobRunner (Spring Boot 안 로컬 Flink)
                              파싱·검증 ──┬──▶ dlq
                              이벤트 시간  │
                     ┌───────────────────┴───────────────────┐
                     ▼                                       ▼
         TradeIntensityService                       DetectionService
         1·5·15분 슬라이딩 집계                    대량 체결 / 급등락 / 거래량 급증
                     │                                       │
                     ▼                                       ▼
             Kafka crypto-metrics                    Kafka crypto-events
                     │                                       │
                     └──────────▶ ResultConsumer ◀───────────┘
                                  │            │
                        MetricsStore(메모리)   PostgreSQL detection_event
                                  │            │
                     REST /api/intensity · /api/events · /api/rules · SSE /api/stream · index.html
```

프로세스는 둘이다. `scripts/producer.py`(Python)와 Spring Boot 애플리케이션. Flink는 별도 클러스터 없이 Spring Boot 프로세스 안에서 로컬 미니 클러스터로 돈다. Kafka와 PostgreSQL은 `docker-compose.yml`로 띄운다.

## 2. 패키지 지도

| 패키지 | 역할 | 주요 클래스 |
| --- | --- | --- |
| `config` | Kafka 소스·싱크 빈, Flink 실행 환경(prototype), Swagger | `FlinkJobConfig`, `SwaggerConfig` |
| `job` | 파이프라인 계약과 Job 실행·재시작 | `FlinkPipeline`, `FlinkJobRunner` |
| `service` | 파이프라인 구현 (Flink 연산을 붙이는 쪽) | `TradeIntensityService`, `DetectionService` |
| `function` | Flink 함수 (파싱, 집계, 감지기, 기준선 조회) | `TradeJsonParser`, `TradeAmountAggregator`, `IntensityWindowFunction`, `LargeTradeDetector`, `PriceMoveDetector`, `VolumeSurgeDetector`, `UpbitCandleBaselineSource`, `ToJsonMapper` |
| `dto` | 스트림을 타는 POJO | `CryptoTrade`, `TradeIntensity`, `DetectionEvent` |
| `rules` | 감지 임계치 규칙 (DB) | `DetectionRule`, `DetectionRules`, `DetectionRuleService`, `DetectionRuleRepository` |
| `consumer` | 결과 토픽을 읽어 저장소에 넣음 | `ResultConsumer` |
| `store` | 조회용 저장소 | `MetricsStore`(메모리), `DetectionEventRepository`(JDBC) |
| `stream` | SSE 연결 관리 | `StreamBroadcaster` |
| `premium` | 김치 프리미엄 (Flink 밖, Spring 스케줄) | `BinancePriceFeed`, `ExchangeRateFeed`, `UpbitLastPriceStore`, `KimchiPremiumService` |
| `orderbook` | 호가 쏠림 (Flink 밖, 호가 토픽 소비) | `Orderbook`, `OrderbookImbalance`, `ImbalanceDetector`, `OrderbookService` |
| `controller` | REST·SSE 엔드포인트 | `IntensityController`, `EventsController`, `RulesController`, `StreamController` |
| `util` | 한국 시간 포맷 | `KstTime` |

경계 규칙 하나: **API 계층은 Flink 상태를 직접 읽지 않는다.** Flink는 Kafka에만 쓰고, Spring은 Kafka에서만 읽는다. 그래서 Job을 다시 띄워도 API는 그대로 살아 있다.

## 3. 데이터 계약

### 3.1 입력: 업비트 trade 메시지

`producer.py`가 받은 바이트를 그대로 `crypto-trades`에 넣는다. 키는 종목 코드라 같은 종목은 한 파티션에 순서대로 쌓인다. `CryptoTrade`가 읽는 필드:

| 필드 | 뜻 | 쓰임 |
| --- | --- | --- |
| `code` | 종목 (`KRW-BTC`) | 모든 keyBy 의 키 |
| `ask_bid` | `BID` 매수 체결, `ASK` 매도 체결 | 매수·매도 금액 구분 |
| `trade_price` × `trade_volume` | 체결 금액(KRW) | 모든 임계치와 지표의 단위 |
| `trade_timestamp` | 체결 시각(ms) | 이벤트 시간. `timestamp`(발송 시각)는 쓰지 않는다 |

모르는 필드는 무시한다(`@JsonIgnoreProperties(ignoreUnknown = true)`). `TradeJsonParser`가 type·code·ask_bid·가격·수량·체결 시각을 검증하고, 실패한 원본은 사이드 출력으로 빼 `crypto-trades-dlq`에 쓴다. 본류에는 null이 흐르지 않는다.

### 3.2 출력 토픽

| 토픽 | 내용 | 생산 주기 | 소비 시작점 |
| --- | --- | --- | --- |
| `crypto-metrics` | `TradeIntensity` JSON. 종목·구간(60/300/900초)별 | 5초마다 종목×구간 | `latest` (옛 지표는 쓸모 없음) |
| `crypto-events` | `DetectionEvent` JSON. `type` 으로 구분 | 조건 충족 시 | `earliest` (이력, 고유 제약으로 중복 제거) |
| `crypto-trades-dlq` | 파싱 실패 원본 문자열 | 실패 시 | 소비자 없음. Kafka UI 로 본다 |
| `crypto-orderbook` | 업비트 orderbook 원본 (producer 가 넣음) | 호가 변경마다 | `latest` (Spring `OrderbookService` 가 직접 읽음, Flink 는 안 읽음) |

`DetectionEvent`는 종류에 없는 필드를 null 로 두고 JSON에서 뺀다. 종류별 필드는 README의 감지 이벤트 절에 있다.

## 4. Flink Job

### 4.1 실행 모델

`FlinkJobRunner`가 `ApplicationRunner`로 기동 뒤 한 번 `launch()`한다.

1. `StreamExecutionEnvironment`를 **새로** 받는다 (prototype 빈). 환경에는 붙인 연산이 쌓이므로 재사용하면 연산이 두 배로 붙는다.
2. Kafka 소스 → `TradeJsonParser` → dlq 싱크 → 이벤트 시간 지정. 이 공통 스트림을 켜진 `FlinkPipeline` 모두에 넘긴다.
3. `env.executeAsync("crypto-tracker")`. `execute()`는 Job이 끝날 때까지 돌아오지 않아 Spring 기동을 막는다.

`restart()`는 `jobClient.cancel()`을 기다린 뒤 1~3을 다시 한다. 규칙 변경이 이 경로를 탄다. 종료 시 `@PreDestroy`로 취소한다.

파이프라인은 `FlinkPipeline` 인터페이스(`name`, `isEnabled`, `attach(DataStream<CryptoTrade>)`)를 구현한 Spring 빈이면 자동으로 붙는다. `jobs.<name>.enabled` 플래그는 각 구현이 읽는다.

### 4.2 이벤트 시간

`FlinkJobRunner.withEventTime`이 `trade_timestamp`를 타임스탬프로, 2초 bounded out-of-orderness 워터마크를 쓴다. 업비트는 종목 안에서는 순서를 지키지만 종목 사이에는 조금 엇갈린다. 처리 시간 대신 이벤트 시간을 쓰는 이유는 재시작 뒤 Kafka에 쌓인 체결을 다시 읽을 때 과거 체결이 "지금"으로 몰려 가짜 급증이 나오지 않게 하려는 것이다. 파티션 유휴 30초를 두어 조용한 파티션이 워터마크를 막지 않게 한다.

워터마크가 2초 뒤에 오므로 윈도우 결과와 감지 이벤트는 체결보다 최소 2초 늦다. 워터마크를 넘겨 늦게 온 체결은 윈도우에서 빠진다(allowed lateness 없음).

### 4.3 체결강도 파이프라인

`TradeIntensityService`는 구간마다 `SlidingEventTimeWindows(구간, 5초)`를 만들고 `TradeAmountAggregator`(증분 누적: 매수·매도 금액과 수량, 건수, 마지막 체결 시각) + `IntensityWindowFunction`(종목 코드와 실제 윈도우 시작·끝 부착)으로 집계한다. 세 구간 결과를 `union`해 하나의 싱크로 보낸다.

슬라이딩 윈도우는 요소 하나를 구간÷슬라이드 개의 윈도우(15분이면 180개)에 넣지만, 증분 집계라 윈도우마다 누적기 하나만 남는다. 체결이 없는 윈도우는 결과를 내지 않으므로 소비 쪽은 `windowEnd`로 신선도를 판단해야 한다.

체결강도 = 매수 ÷ 매도 × 100. 매도가 0이면 9999.99로 막는다(`TradeIntensity.MAX_INTENSITY`). 항상 0~100인 `buyRatio`도 같이 낸다.

### 4.4 감지 파이프라인

`DetectionService`는 `attach` 시점에 `RulesProvider.current()`로 규칙 스냅샷을 받아 감지기 생성자에 싣는다. 감지기는 체결마다 `rules.resolve(code)`로 종목 규칙을 찾는다. 세 감지기 모두 `KeyedProcessFunction`(키 = 종목)이고 상태는 다음과 같다.

| 감지기 | 입력 | 상태 | 판정 |
| --- | --- | --- | --- |
| `LargeTradeDetector` | 체결 | 방향별 최근 10초 체결 목록, 조건·방향별 마지막 발생 시각 | 단일 ≥ `largeSingleAmount` → `LARGE_TRADE`. 단일에 안 걸린 체결의 10초 같은 방향 합 ≥ `largeBurstAmount` → `LARGE_TRADE_BURST`. 쿨다운 `largeCooldownSeconds` |
| `PriceMoveDetector` | 체결 | 최근 5분 체결가 목록, 방향별 마지막 발생 시각·가격 | 구간 안 가장 오래된 가격 대비 ±`priceThresholdPercent` → `PRICE_SURGE`/`PRICE_DROP`. 쿨다운 중이라도 직전 이벤트 가격에서 임계치만큼 더 가면 낸다 |
| `VolumeSurgeDetector` | 5분 슬라이딩 `TradeIntensity` | 정렬 버킷(5분 경계) 거래대금 최근 288개, 마지막 버킷 끝, 선채움 여부, 마지막 발생 시각 | 현재 5분 거래대금 ≥ 버킷 평균 × `volumeMultiplier` → `VOLUME_SURGE`. 버킷 12개 미만이면 판정 안 함 |

거래량 감지는 체결강도와 같은 5분 윈도우를 **다시** 집계한다. 파이프라인끼리 스트림을 넘기지 않아야 한쪽만 끄거나 바꿔도 다른 쪽이 영향을 받지 않는다. 기준선은 종목의 첫 윈도우에서 `UpbitCandleBaselineSource`가 업비트 5분봉 200개를 REST로 가져와 채운다. 실패하면 경고만 남기고 스트림으로 쌓는다.

상태는 모두 heap 에 있고 체크포인트는 메모리에 남는다. 재시작하면 상태는 사라진다. 쿨다운과 가격 이력은 몇 분 안에 다시 차고 기준선은 선채움으로 돌아오므로 받아들였다.

## 5. API 계층

- `ResultConsumer`: spring-kafka 리스너 둘. 지표는 `MetricsStore.put`(종목·구간별 최신 `windowEnd`만 남김) 뒤 SSE로 전파. 이벤트는 `DetectionEventRepository.save` 가 true(중복 아님)일 때만 SSE로 전파.
- `DetectionEventRepository`: `(type, code, event_time)` 고유 제약. 중복 삽입은 `DuplicateKeyException`을 잡아 false 로 돌린다. 스키마는 `schema.sql`이 기동마다 실행(`IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`).
- `StreamBroadcaster`: `SseEmitter` 목록. 연결 직후 `StreamController`가 `snapshot`(지표 전체 + 최근 이벤트 50건)을 보내고, 이후 `metric`·`event`. 15초마다 주석 하트비트. 보내다 실패하면 목록에서 뺀다.
- `static/index.html`: 의존성 없는 한 장. `EventSource`가 끊기면 브라우저가 재접속하고 snapshot으로 다시 채운다.

### 5.1 김치 프리미엄 (Flink 밖)

업비트 마지막 체결가(`UpbitLastPriceStore`, 원본 토픽을 latest 부터 읽음), 바이낸스 USDT 최신가(`BinancePriceFeed`, Java 11 `HttpClient` WebSocket 으로 miniTicker 구독, 끊기면 백오프 재접속), 환율(`ExchangeRateFeed`, 10분마다 두나무 → er-api)을 `KimchiPremiumService`가 5초마다 합친다. 셋 다 느리게 바뀌는 값의 결합이라 스트림 조인이 필요 없다. 한쪽이 2분 넘게 멈추면 그 종목은 건너뛴다. 멈춘 가격으로 계산하면 괴리율이 거짓으로 커지기 때문이다. 임계치 초과는 `KIMCHI_PREMIUM` 이벤트로 저장소에 직접 넣고 SSE 로 전파한다(Kafka events 토픽은 거치지 않는다).

### 5.2 호가 쏠림 (Flink 밖)

`OrderbookService`가 `crypto-orderbook`을 latest 부터 읽어 종목별 `OrderbookImbalance`(총 매수÷매도 잔량, 상위 N호가 금액, 스프레드)를 유지한다. 호가는 상태 스냅샷이라 윈도우 집계가 필요 없다. 대시보드에는 종목당 1초에 한 번만 보낸다. `ImbalanceDetector`는 쏠림이 `hold-seconds` 동안 끊기지 않고 이어졌을 때 한 번 `ORDERBOOK_IMBALANCE`를 내고 종목·방향별 쿨다운을 둔다. 순간값은 노이즈라서다. 상태는 메모리뿐이고 Kafka 리스너 스레드 하나에서만 쓰므로 동기화하지 않는다.

## 6. 감지 규칙과 Job 재시작

`detection_rule` 테이블에 기본 규칙 `*` 한 줄과 종목별 덮어쓰기가 있다. 덮어쓰기는 바꾼 값만 채우고 나머지는 null(기본값 따름). `DetectionRules.resolve(code)`가 둘을 합친다.

`application.yml`의 `detection.*` 임계치는 DB에 `*`가 없을 때 `DetectionRuleService`가 한 번 심는 초깃값이다. 그 뒤로는 `/api/rules`가 진실이다. 구간 길이(10초·5분·24시간)는 윈도우 구조라 규칙이 아니라 설정 파일에 있다.

규칙을 바꾸면 `DetectionRuleService`가 저장 뒤 `FlinkJobRunner.restart()`를 부른다. 러너와 규칙 서비스는 서로를 참조하므로(러너 → 파이프라인 → 규칙 서비스 → 러너) 규칙 서비스 쪽을 `@Lazy`로 받는다. Job 공백은 1~3초이고 API와 대시보드는 끊기지 않는다.

Broadcast State 로 재시작 없이 반영하는 방식은 감지기 셋을 `BroadcastProcessFunction`으로 다시 써야 해서 미뤘다. 규칙 변경이 잦아지면 그때 옮긴다.

## 7. 빌드·테스트·배포

배포는 `docker compose up -d --build` 한 번이다. `Dockerfile`이 Gradle 이미지에서 `bootJar`를 만들고 JRE 이미지에서 실행한다. 이때 jar를 풀어 `java -cp classes:lib/* ...`로 띄운다. fat jar를 `java -jar`로 띄우면 Spring Boot 의 중첩 jar 클래스로더 때문에 Flink 로컬 JobMaster 가 시스템 클래스로더에서 `ExecutionConfig`를 못 찾아 Job 이 시작되지 않는다. `scripts/Dockerfile`은 producer 이미지다. `kafka-init` 서비스가 토픽을 만든 뒤에야 앱과 producer 가 뜬다.

로컬에 JDK 17이 없어도 Docker로 돌릴 수 있다.

```bash
# 테스트 (Gradle 캐시 볼륨을 재사용)
docker run --rm -v "$PWD":/home/gradle/project -v crypto-tracker-gradle-cache:/home/gradle/.gradle \
  -w /home/gradle/project gradle:8.5-jdk17 gradle test --no-daemon

# Kafka·PostgreSQL 을 compose 로 띄운 뒤 앱을 같은 네트워크에서 실행
docker compose up -d zookeeper kafka postgres
docker run -d --name crypto-app --network crypto_tracker_dev_default -p 8082:8082 \
  -v "$PWD":/home/gradle/project -v crypto-tracker-gradle-cache:/home/gradle/.gradle \
  -w /home/gradle/project gradle:8.5-jdk17 gradle bootRun --no-daemon -q \
  --args="--kafka.bootstrap-servers=kafka:29092 --spring.datasource.url=jdbc:postgresql://postgres:5432/crypto"
```

테스트 종류:

| 대상 | 방법 |
| --- | --- |
| 집계·규칙 병합·파싱 검증 | 순수 단위 테스트 |
| 감지기 셋 | `ProcessFunctionTestHarnesses.forKeyedProcessFunction` 으로 체결을 넣고 출력 확인 (`flink-test-utils`) |
| 체결강도 파이프라인 | 로컬 환경 `fromElements` + `executeAndCollect` 로 실제 윈도우 집계 |
| 저장소 | `@JdbcTest` + H2. `schema.sql` 을 그대로 실행 |

Flink 1.18 API 기준이다. `env.fromData`(1.19)는 없고 `fromElements`를 쓴다.

## 8. 설계 결정 요약

| 결정 | 이유 |
| --- | --- |
| `ticker` 대신 `trade` 구독 | 체결강도에는 체결 단위의 매수·매도 구분(`ask_bid`)이 필요하다. `ticker`의 `change`는 전일 대비 등락이라 쓸 수 없다 |
| 모든 임계치를 원화 금액으로 | 종목마다 수량 단위가 달라 "0.5 BTC" 같은 기준은 다른 종목에 쓸 수 없다 |
| 이벤트 시간 윈도우 | 재시작 뒤 Kafka 재생 시 과거 체결이 현재로 몰리지 않게 |
| 파이프라인 등록 + 러너 1회 실행 | 서비스마다 `execute()` 하면 같은 토픽을 여러 번 읽고 두 번째 Job 은 시작도 못 한다 |
| 지표·이벤트 토픽 분리 | 소비 시작점과 보존 정책이 다르다 (지표 latest, 이벤트 earliest) |
| 파이프라인 간 스트림 미공유 | 한쪽을 끄거나 바꿔도 다른 쪽에 영향 없게. 5분 윈도우 중복 비용은 작다 |
| API 는 Kafka 만 읽음 | Job 재시작과 API 가동을 분리 |
| 규칙 변경은 Job 재시작 | Broadcast State 보다 코드가 훨씬 적고, 상태 초기화 비용이 작다 |
| JPA 대신 JdbcTemplate | 테이블 둘에 ORM 은 과하다 |

## 9. 알려진 한계

- 메신저 알림은 범위 밖. 이벤트는 대시보드·API 에서만 본다. 붙인다면 `ResultConsumer.onEvent` 뒤에 발송기를 둔다.
- Job 재시작 시 쿨다운·가격 이력·기준선 상태가 초기화된다.
- `crypto-trades` 파티션이 하나라 병렬도 1 이다. 종목이 늘면 파티션과 `flink.parallelism` 을 함께 올린다.
- spring-kafka 가 kafka-clients 를 3.6.0 으로 올린다. Flink 커넥터 3.0.2 는 3.2.3 기준이지만 at-least-once 싱크만 써서 문제가 없었다. exactly-once 로 바꾸면 다시 확인한다.
- 워터마크 2초만큼 결과가 늦다. 더 빠른 감지가 필요하면 대량 체결 감지만 처리 시간으로 옮기는 것을 검토한다.
- `SwaggerConfig` 의 연락처·운영 서버 주소는 자리 표시자다.

## 10. 기능을 더할 때

1. 새 파이프라인: `FlinkPipeline` 구현 `@Service` 를 만들고 `jobs.<name>.enabled` 를 읽는다. 필요한 싱크는 `FlinkJobConfig.stringSink(topic)` 으로 빈을 추가한다.
2. 새 감지 종류: `DetectionEvent` 에 type 상수와 팩토리, 필요한 필드를 더하고 `schema.sql` 에 `ADD COLUMN IF NOT EXISTS`, `DetectionEventRepository` 의 insert·mapRow 를 맞춘다. 대시보드 배지 색은 `index.html` 의 `.badge.<TYPE>`.
3. 새 임계치: `DetectionRule` 필드·`overriddenBy`·`missingOrInvalidField`, `detection_rule` 컬럼, 저장소 upsert·mapRow, `DetectionRuleService` 의 초깃값.
4. 감지기는 반드시 하네스 테스트를 같이 둔다. 실데이터로는 임계치를 넘는 순간을 기다릴 수 없다.
