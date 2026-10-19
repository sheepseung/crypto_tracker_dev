package com.crypto.flink.scorecard;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.premium.UpbitLastPriceStore;
import com.crypto.flink.store.DetectionEventRepository;
import com.crypto.flink.store.PriceTickRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * G2 신호 성적표의 재료를 채운다.
 * 30초마다 종목별 마지막 체결가를 틱으로 남기고, 1분마다 이벤트 뒤 5·15·60분 가격을 찾아 수익률(%)을 넣는다.
 * 수익률은 이벤트 시점 가격 대비이며, 그 시점 가격은 이벤트 자체에 있으면 그것을, 없으면 가장 가까운 틱을 쓴다.
 */
@Component
public class OutcomeFiller {

    private static final Logger logger = LoggerFactory.getLogger(OutcomeFiller.class);
    static final long[] HORIZONS_MILLIS = {5 * 60_000L, 15 * 60_000L, 60 * 60_000L};
    // 틱이 30초 간격이라 이 안에 하나는 있다. 수집이 끊긴 구간은 비워 둔다
    private static final long TICK_TOLERANCE_MILLIS = 90_000;
    private static final int BATCH = 200;

    private final UpbitLastPriceStore lastPrices;
    private final PriceTickRepository ticks;
    private final DetectionEventRepository events;
    private final List<String> codes;
    private final int retentionDays;

    public OutcomeFiller(UpbitLastPriceStore lastPrices, PriceTickRepository ticks, DetectionEventRepository events,
            @Value("${premium.codes:KRW-BTC,KRW-ETH,KRW-DOGE}") List<String> codes,
            @Value("${scorecard.tick-retention-days:14}") int retentionDays) {
        this.lastPrices = lastPrices;
        this.ticks = ticks;
        this.events = events;
        this.codes = codes;
        this.retentionDays = retentionDays;
    }

    @Scheduled(fixedRate = 30_000)
    public void recordTicks() {
        long ts = System.currentTimeMillis() / 30_000 * 30_000;
        for (String code : codes) {
            lastPrices.latest(code).ifPresent(p -> ticks.save(code, ts, p.price()));
        }
    }

    @Scheduled(fixedDelay = 60_000, initialDelay = 20_000)
    public void fillOutcomes() {
        long now = System.currentTimeMillis();
        List<DetectionEvent> pending = events.findOutcomePending(now - HORIZONS_MILLIS[0], BATCH);
        int filled = 0;
        for (DetectionEvent e : pending) {
            if (fill(e, now)) {
                filled++;
            }
        }
        if (filled > 0) {
            logger.info("성적 채움 {}건", filled);
        }
    }

    /** 채울 수 있는 지평만 채운다. 60분까지 다 채우면 true. */
    boolean fill(DetectionEvent e, long now) {
        Double priceAt = e.getPriceAt() != null ? e.getPriceAt()
                : e.getPrice() != null ? e.getPrice()
                : ticks.priceNear(e.getCode(), e.getEventTime(), TICK_TOLERANCE_MILLIS).orElse(null);
        if (priceAt == null || priceAt <= 0) {
            // 틱이 없으면 영영 못 채우므로 60분을 0 이 아닌 '없음' 으로 남겨 두고 다음 틱 수집을 기다린다
            return false;
        }
        Double[] returns = new Double[3];
        for (int i = 0; i < HORIZONS_MILLIS.length; i++) {
            long at = e.getEventTime() + HORIZONS_MILLIS[i];
            if (at > now) {
                break;
            }
            returns[i] = ticks.priceNear(e.getCode(), at, TICK_TOLERANCE_MILLIS)
                    .map(p -> (p / priceAt - 1.0) * 100.0).orElse(null);
        }
        events.updateOutcome(e.getId(), priceAt, returns[0], returns[1], returns[2]);
        return returns[2] != null;
    }

    @Scheduled(cron = "0 30 4 * * *", zone = "Asia/Seoul")
    public void pruneTicks() {
        int deleted = ticks.deleteOlderThan(System.currentTimeMillis() - retentionDays * 86_400_000L);
        logger.info("가격 틱 정리 {}행", deleted);
    }
}
