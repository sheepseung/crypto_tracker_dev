package com.crypto.flink.premium;

import com.crypto.flink.dto.CryptoTrade;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 업비트 종목별 마지막 체결가. 프리미엄 계산용이라 원본 체결 토픽을 최신 위치부터 그대로 읽는다.
 */
@Component
public class UpbitLastPriceStore {

    private static final Logger logger = LoggerFactory.getLogger(UpbitLastPriceStore.class);

    private final ObjectMapper objectMapper;
    private final Map<String, PriceAt> prices = new ConcurrentHashMap<>();

    public record PriceAt(double price, long timestamp) {
    }

    public UpbitLastPriceStore(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${kafka.input-topic}", groupId = "${kafka.api-group-id}",
            properties = "auto.offset.reset=latest")
    public void onTrade(String json) {
        try {
            CryptoTrade trade = objectMapper.readValue(json, CryptoTrade.class);
            if (trade.getCode() == null || trade.getTradePrice() == null || trade.getTradeTimestamp() == null) {
                return;
            }
            put(trade.getCode(), trade.getTradePrice(), trade.getTradeTimestamp());
        } catch (JsonProcessingException e) {
            logger.debug("체결 메시지 파싱 실패 (프리미엄용): {}", json);
        }
    }

    public void put(String code, double price, long timestamp) {
        prices.merge(code, new PriceAt(price, timestamp),
                (current, incoming) -> incoming.timestamp() >= current.timestamp() ? incoming : current);
    }

    public Optional<PriceAt> latest(String code) {
        return Optional.ofNullable(prices.get(code));
    }
}
