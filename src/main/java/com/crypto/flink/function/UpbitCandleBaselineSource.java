package com.crypto.flink.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 업비트 분봉 REST 로 과거 구간별 거래대금을 가져온다.
 * 기준선을 24시간 동안 스트림으로만 쌓으면 그동안 급증을 못 잡으므로 기동 직후에 한 번 채운다.
 */
public class UpbitCandleBaselineSource implements BaselineSource {

    private static final Logger logger = LoggerFactory.getLogger(UpbitCandleBaselineSource.class);
    private static final String ENDPOINT = "https://api.upbit.com/v1/candles/minutes/%d?market=%s&count=%d";
    // 업비트 분봉 조회는 한 번에 200개까지만 준다
    private static final int MAX_COUNT = 200;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private transient HttpClient httpClient;
    private transient ObjectMapper objectMapper;

    @Override
    public List<Double> recentBucketAmounts(String code, int bucketSeconds, int limit) {
        int unitMinutes = bucketSeconds / 60;
        int count = Math.min(limit, MAX_COUNT);
        String url = String.format(ENDPOINT, unitMinutes, code, count);
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(TIMEOUT)
                    .header("Accept", "application/json")
                    .build();
            HttpResponse<String> response = client().send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                logger.warn("기준선 조회 실패 {}: HTTP {}", code, response.statusCode());
                return List.of();
            }
            List<Double> amounts = new ArrayList<>();
            for (JsonNode candle : mapper().readTree(response.body())) {
                amounts.add(candle.path("candle_acc_trade_price").asDouble());
            }
            // 업비트는 최신 봉부터 주므로 오래된 순으로 뒤집는다
            Collections.reverse(amounts);
            logger.info("기준선 조회 {}: {}분봉 {}개", code, unitMinutes, amounts.size());
            return amounts;
        } catch (IOException | RuntimeException e) {
            logger.warn("기준선 조회 실패 {}: {}", code, e.toString());
            return List.of();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }

    private HttpClient client() {
        if (httpClient == null) {
            httpClient = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
        }
        return httpClient;
    }

    private ObjectMapper mapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }
}
