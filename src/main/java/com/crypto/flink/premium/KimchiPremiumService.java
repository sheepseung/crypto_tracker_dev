package com.crypto.flink.premium;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.store.DetectionEventRepository;
import com.crypto.flink.stream.StreamBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * F8 김치 프리미엄. 업비트 마지막 체결가, 바이낸스 최신가, 환율을 5초마다 합쳐 종목별 괴리율을 만든다.
 * 셋 다 느리게 바뀌는 값이라 Flink 를 거치지 않고 여기서 계산한다.
 * 괴리율 절댓값이 임계치를 넘으면 KIMCHI_PREMIUM 이벤트를 이력에 남긴다.
 */
@Service
public class KimchiPremiumService {

    private static final Logger logger = LoggerFactory.getLogger(KimchiPremiumService.class);
    // 이 시간보다 오래된 가격으로는 계산하지 않는다. 한쪽이 멈추면 괴리율이 거짓으로 커진다.
    // DOGE 처럼 체결이 몇 분씩 끊기는 종목이 빠지지 않도록 10분으로 둔다
    private static final long MAX_PRICE_AGE_MILLIS = 600_000;

    private final boolean enabled;
    private final List<String> codes;
    private final double thresholdPercent;
    private final long cooldownMillis;
    private final UpbitLastPriceStore upbitPrices;
    private final BinancePriceFeed binancePrices;
    private final ExchangeRateFeed exchangeRate;
    private final DetectionEventRepository eventRepository;
    private final StreamBroadcaster broadcaster;

    private final Map<String, KimchiPremium> latest = new ConcurrentHashMap<>();
    private final Map<String, Long> lastEmitted = new ConcurrentHashMap<>();
    // G6 김프 급변용 최근 스냅샷 (종목별, 오래된 순)
    private final Map<String, java.util.ArrayDeque<KimchiPremium>> recent = new ConcurrentHashMap<>();
    private final Map<String, Long> lastJumpEmitted = new ConcurrentHashMap<>();
    private final double jumpThresholdPoints;
    private final long jumpWindowMillis;
    private final long jumpCooldownMillis;

    public KimchiPremiumService(
            @Value("${premium.enabled:true}") boolean enabled,
            @Value("${premium.codes:KRW-BTC,KRW-ETH,KRW-DOGE}") List<String> codes,
            @Value("${premium.threshold-percent:5.0}") double thresholdPercent,
            @Value("${premium.cooldown-seconds:1800}") int cooldownSeconds,
            @Value("${premium.jump.threshold-points:0.5}") double jumpThresholdPoints,
            @Value("${premium.jump.window-seconds:300}") int jumpWindowSeconds,
            @Value("${premium.jump.cooldown-seconds:900}") int jumpCooldownSeconds,
            UpbitLastPriceStore upbitPrices, BinancePriceFeed binancePrices, ExchangeRateFeed exchangeRate,
            DetectionEventRepository eventRepository, StreamBroadcaster broadcaster) {
        this.enabled = enabled;
        this.codes = codes;
        this.thresholdPercent = thresholdPercent;
        this.cooldownMillis = cooldownSeconds * 1000L;
        this.jumpThresholdPoints = jumpThresholdPoints;
        this.jumpWindowMillis = jumpWindowSeconds * 1000L;
        this.jumpCooldownMillis = jumpCooldownSeconds * 1000L;
        this.upbitPrices = upbitPrices;
        this.binancePrices = binancePrices;
        this.exchangeRate = exchangeRate;
        this.eventRepository = eventRepository;
        this.broadcaster = broadcaster;
    }

    public List<KimchiPremium> current() {
        return latest.values().stream().sorted(Comparator.comparing(KimchiPremium::code)).toList();
    }

    @Scheduled(fixedDelayString = "${premium.refresh-seconds:5}000")
    public void refresh() {
        if (!enabled) {
            return;
        }
        ExchangeRateFeed.Rate rate = exchangeRate.latest().orElse(null);
        if (rate == null) {
            return;
        }
        long now = System.currentTimeMillis();
        for (String code : codes) {
            compute(code, rate, now).ifPresent(premium -> {
                latest.put(code, premium);
                broadcaster.publishPremium(premium);
                detect(premium, now);
                detectJump(premium, now);
            });
        }
    }

    /** G6: 5분 전 스냅샷과 비교해 김프가 임계치 이상 변했으면 이벤트. 어느 쪽이 앞서 움직였는지 같이 적는다. */
    private void detectJump(KimchiPremium now, long nowMillis) {
        java.util.ArrayDeque<KimchiPremium> history = recent.computeIfAbsent(now.code(), c -> new java.util.ArrayDeque<>());
        history.addLast(now);
        while (!history.isEmpty() && nowMillis - history.peekFirst().updatedAt() > jumpWindowMillis + 10_000) {
            history.pollFirst();
        }
        jumpOf(history, now, jumpWindowMillis, jumpThresholdPoints).ifPresent(event -> {
            Long last = lastJumpEmitted.get(now.code());
            if (last != null && nowMillis - last < jumpCooldownMillis) {
                return;
            }
            lastJumpEmitted.put(now.code(), nowMillis);
            if (eventRepository.save(event)) {
                logger.info("감지 이벤트: {}", event.getMessage());
                broadcaster.publishEvent(event);
            }
        });
    }

    /** 창 길이에 가장 가까운 과거 스냅샷을 기준으로 변화폭을 본다. 과거가 창의 절반도 안 되면 판정하지 않는다. */
    static java.util.Optional<DetectionEvent> jumpOf(java.util.Deque<KimchiPremium> history, KimchiPremium now,
            long windowMillis, double thresholdPoints) {
        KimchiPremium before = null;
        for (KimchiPremium p : history) {
            if (now.updatedAt() - p.updatedAt() >= windowMillis / 2) {
                before = p;
                break;
            }
        }
        if (before == null) {
            return java.util.Optional.empty();
        }
        double delta = now.premiumPercent() - before.premiumPercent();
        if (Math.abs(delta) < thresholdPoints) {
            return java.util.Optional.empty();
        }
        double upbitMove = (now.upbitPrice() / before.upbitPrice() - 1.0) * 100.0;
        double globalMove = (now.globalPriceKrw() / before.globalPriceKrw() - 1.0) * 100.0;
        return java.util.Optional.of(DetectionEvent.premiumJump(now.code(), before.premiumPercent(),
                now.premiumPercent(), upbitMove, globalMove, now.updatedAt()));
    }

    private java.util.Optional<KimchiPremium> compute(String code, ExchangeRateFeed.Rate rate, long now) {
        UpbitLastPriceStore.PriceAt upbit = upbitPrices.latest(code).orElse(null);
        BinancePriceFeed.PriceAt binance = binancePrices.latest(BinancePriceFeed.symbolFor(code)).orElse(null);
        if (upbit == null || binance == null) {
            return java.util.Optional.empty();
        }
        if (now - upbit.timestamp() > MAX_PRICE_AGE_MILLIS || now - binance.timestamp() > MAX_PRICE_AGE_MILLIS) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(
                KimchiPremium.of(code, upbit.price(), binance.price(), rate.usdKrw(), rate.source(), now));
    }

    private void detect(KimchiPremium premium, long now) {
        if (Math.abs(premium.premiumPercent()) < thresholdPercent) {
            return;
        }
        Long last = lastEmitted.get(premium.code());
        if (last != null && now - last < cooldownMillis) {
            return;
        }
        lastEmitted.put(premium.code(), now);
        DetectionEvent event = DetectionEvent.kimchiPremium(premium);
        if (eventRepository.save(event)) {
            logger.info("감지 이벤트: {}", event.getMessage());
            broadcaster.publishEvent(event);
        }
    }
}
