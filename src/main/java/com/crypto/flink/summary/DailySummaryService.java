package com.crypto.flink.summary;

import com.crypto.flink.store.DetectionEventRepository;
import com.crypto.flink.store.IntensityBucketRepository;
import com.crypto.flink.stream.StreamBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * F10 일별 요약. 저장된 5분 버킷과 감지 이벤트로 하루(KST)를 집계한다.
 * 매일 00:05 에 전날 요약을 로그와 SSE 로 내보낸다. 메신저 전송은 범위 밖이라 여기까지다.
 */
@Service
public class DailySummaryService {

    private static final Logger logger = LoggerFactory.getLogger(DailySummaryService.class);
    public static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final int BUCKETS_PER_DAY = 24 * 3600 / IntensityBucketRepository.BUCKET_SECONDS;
    // 수집 지연이나 재시작 틈을 감안해 이 비율 이상 모이면 하루치로 본다
    private static final double COMPLETE_COVERAGE = 0.95;

    private final IntensityBucketRepository buckets;
    private final DetectionEventRepository events;
    private final StreamBroadcaster broadcaster;

    public DailySummaryService(IntensityBucketRepository buckets, DetectionEventRepository events,
            StreamBroadcaster broadcaster) {
        this.buckets = buckets;
        this.events = events;
        this.broadcaster = broadcaster;
    }

    public DailySummary forDate(LocalDate date) {
        long from = date.atStartOfDay(KST).toInstant().toEpochMilli();
        long to = date.plusDays(1).atStartOfDay(KST).toInstant().toEpochMilli();

        List<DailySummary.CodeSummary> codes = buckets.totalsBetween(from, to).stream()
                .map(t -> new DailySummary.CodeSummary(t.code(), t.buyAmount(), t.sellAmount(), t.totalAmount(),
                        t.netAmount(), t.intensity(), t.tradeCount(), t.buckets(),
                        (double) t.buckets() / BUCKETS_PER_DAY))
                .toList();

        String topNetBuy = codes.stream().max(Comparator.comparingDouble(DailySummary.CodeSummary::netAmount))
                .map(DailySummary.CodeSummary::code).orElse(null);
        String topVolume = codes.stream().max(Comparator.comparingDouble(DailySummary.CodeSummary::totalAmount))
                .map(DailySummary.CodeSummary::code).orElse(null);

        Map<String, Integer> byType = new TreeMap<>();
        Map<String, Integer> byCode = new TreeMap<>();
        int total = 0;
        for (DetectionEventRepository.TypeCodeCount row : events.countBetween(from, to)) {
            byType.merge(row.type(), row.count(), Integer::sum);
            byCode.merge(row.code(), row.count(), Integer::sum);
            total += row.count();
        }

        boolean complete = !codes.isEmpty()
                && codes.stream().allMatch(c -> c.coverage() >= COMPLETE_COVERAGE);
        return new DailySummary(date.toString(), from, to, codes, topNetBuy, topVolume, total, byType, byCode, complete);
    }

    public DailySummary today() {
        return forDate(LocalDate.now(KST));
    }

    @Scheduled(cron = "0 5 0 * * *", zone = "Asia/Seoul")
    public void publishYesterday() {
        DailySummary summary = forDate(LocalDate.now(KST).minusDays(1));
        logger.info("일별 요약 {}: 이벤트 {}건 {}, 최대 순매수 {}, 거래대금 1위 {}, 완전성 {}",
                summary.date(), summary.eventCount(), summary.eventsByType(), summary.topNetBuyCode(),
                summary.topVolumeCode(), summary.complete());
        broadcaster.publishSummary(summary);
    }
}
