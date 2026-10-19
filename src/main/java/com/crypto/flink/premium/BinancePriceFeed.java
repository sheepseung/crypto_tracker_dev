package com.crypto.flink.premium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 바이낸스 공개 WebSocket 의 miniTicker 로 USDT 마켓 최신가를 받는다. 키가 필요 없고 1초마다 온다.
 * 끊기면 1초부터 30초까지 늘려 가며 다시 붙는다.
 */
@Component
public class BinancePriceFeed implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(BinancePriceFeed.class);
    private static final String STREAM_BASE = "wss://stream.binance.com:9443/stream?streams=";
    private static final long MAX_RECONNECT_SECONDS = 30;

    private final boolean enabled;
    private final List<String> symbols;
    private final ObjectMapper objectMapper;
    private final Map<String, PriceAt> prices = new ConcurrentHashMap<>();
    private final ScheduledExecutorService reconnects = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "binance-feed");
        t.setDaemon(true);
        return t;
    });
    private final StringBuilder partial = new StringBuilder();

    private volatile WebSocket webSocket;
    private volatile boolean stopped;
    private long reconnectSeconds = 1;

    public record PriceAt(double price, long timestamp) {
    }

    public BinancePriceFeed(
            @Value("${premium.enabled:true}") boolean enabled,
            @Value("${premium.binance-symbols:BTCUSDT,ETHUSDT,DOGEUSDT}") List<String> symbols,
            ObjectMapper objectMapper) {
        this.enabled = enabled;
        this.symbols = symbols;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (enabled) {
            connect();
        }
    }

    public Optional<PriceAt> latest(String symbol) {
        return Optional.ofNullable(prices.get(symbol));
    }

    /** 업비트 종목 코드(KRW-BTC)를 바이낸스 심볼(BTCUSDT)로 바꾼다. */
    public static String symbolFor(String upbitCode) {
        return upbitCode.replace("KRW-", "") + "USDT";
    }

    /** miniTicker 메시지에서 심볼과 종가를 뽑는다. 다른 메시지면 비어 있다. */
    static Optional<Map.Entry<String, PriceAt>> parse(ObjectMapper mapper, String json) {
        try {
            JsonNode data = mapper.readTree(json).path("data");
            if (!"24hrMiniTicker".equals(data.path("e").asText())) {
                return Optional.empty();
            }
            String symbol = data.path("s").asText();
            double close = data.path("c").asDouble();
            long time = data.path("E").asLong();
            if (symbol.isEmpty() || close <= 0) {
                return Optional.empty();
            }
            return Optional.of(Map.entry(symbol, new PriceAt(close, time)));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private void connect() {
        if (stopped) {
            return;
        }
        String streams = String.join("/", symbols.stream().map(s -> s.toLowerCase() + "@miniTicker").toList());
        URI uri = URI.create(STREAM_BASE + streams);
        HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(uri, new Listener())
                .whenComplete((ws, error) -> {
                    if (error != null) {
                        logger.warn("바이낸스 연결 실패: {} ({}초 뒤 재시도)", error.toString(), reconnectSeconds);
                        scheduleReconnect();
                    } else {
                        webSocket = ws;
                        reconnectSeconds = 1;
                        logger.info("바이낸스 miniTicker 구독: {}", symbols);
                    }
                });
    }

    private void scheduleReconnect() {
        if (stopped) {
            return;
        }
        reconnects.schedule(this::connect, reconnectSeconds, TimeUnit.SECONDS);
        reconnectSeconds = Math.min(reconnectSeconds * 2, MAX_RECONNECT_SECONDS);
    }

    @PreDestroy
    public void stop() {
        stopped = true;
        if (webSocket != null) {
            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown");
        }
        reconnects.shutdownNow();
    }

    private class Listener implements WebSocket.Listener {

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            // 큰 프레임은 조각으로 오므로 마지막 조각까지 모아서 파싱한다
            partial.append(data);
            if (last) {
                String message = partial.toString();
                partial.setLength(0);
                parse(objectMapper, message).ifPresent(entry -> prices.put(entry.getKey(), entry.getValue()));
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            logger.warn("바이낸스 연결 종료 ({} {}), 재접속", statusCode, reason);
            scheduleReconnect();
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            logger.warn("바이낸스 연결 오류: {}, 재접속", error.toString());
            scheduleReconnect();
        }
    }
}
