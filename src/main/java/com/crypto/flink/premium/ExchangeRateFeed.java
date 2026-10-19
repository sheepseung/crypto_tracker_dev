package com.crypto.flink.premium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * USD/KRW 환율. 업비트 운영사(두나무)의 공개 환율을 먼저 쓰고, 안 되면 open.er-api.com 으로 대신한다.
 * 환율은 분 단위로 조금씩 움직이므로 10분마다만 갱신한다.
 */
@Component
public class ExchangeRateFeed {

    private static final Logger logger = LoggerFactory.getLogger(ExchangeRateFeed.class);
    private static final String DUNAMU_URL = "https://quotation-api-cdn.dunamu.com/v1/forex/recent?codes=FRX.KRWUSD";
    private static final String FALLBACK_URL = "https://open.er-api.com/v6/latest/USD";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final boolean enabled;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();

    private volatile Rate rate;

    public record Rate(double usdKrw, String source, long updatedAt) {
    }

    public ExchangeRateFeed(@Value("${premium.enabled:true}") boolean enabled, ObjectMapper objectMapper) {
        this.enabled = enabled;
        this.objectMapper = objectMapper;
    }

    public Optional<Rate> latest() {
        return Optional.ofNullable(rate);
    }

    @Scheduled(initialDelay = 0, fixedDelayString = "${premium.fx-refresh-seconds:600}000")
    public void refresh() {
        if (!enabled) {
            return;
        }
        OptionalDouble dunamu = fetch(DUNAMU_URL).map(body -> parseDunamu(objectMapper, body))
                .orElse(OptionalDouble.empty());
        if (dunamu.isPresent()) {
            rate = new Rate(dunamu.getAsDouble(), "dunamu", System.currentTimeMillis());
            logger.info("환율 갱신 USD/KRW {} (dunamu)", rate.usdKrw());
            return;
        }
        OptionalDouble fallback = fetch(FALLBACK_URL).map(body -> parseErApi(objectMapper, body))
                .orElse(OptionalDouble.empty());
        if (fallback.isPresent()) {
            rate = new Rate(fallback.getAsDouble(), "er-api", System.currentTimeMillis());
            logger.info("환율 갱신 USD/KRW {} (er-api)", rate.usdKrw());
            return;
        }
        logger.warn("환율 조회 실패. 이전 값 유지: {}", rate);
    }

    static OptionalDouble parseDunamu(ObjectMapper mapper, String body) {
        try {
            JsonNode first = mapper.readTree(body).path(0);
            double base = first.path("basePrice").asDouble();
            return base > 0 ? OptionalDouble.of(base) : OptionalDouble.empty();
        } catch (Exception e) {
            return OptionalDouble.empty();
        }
    }

    static OptionalDouble parseErApi(ObjectMapper mapper, String body) {
        try {
            double krw = mapper.readTree(body).path("rates").path("KRW").asDouble();
            return krw > 0 ? OptionalDouble.of(krw) : OptionalDouble.empty();
        } catch (Exception e) {
            return OptionalDouble.empty();
        }
    }

    private Optional<String> fetch(String url) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(TIMEOUT)
                    .header("Accept", "application/json")
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                logger.warn("환율 조회 실패 {}: HTTP {}", url, response.statusCode());
                return Optional.empty();
            }
            return Optional.of(response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (Exception e) {
            logger.warn("환율 조회 실패 {}: {}", url, e.toString());
            return Optional.empty();
        }
    }
}
