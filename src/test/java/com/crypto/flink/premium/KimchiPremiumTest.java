package com.crypto.flink.premium;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KimchiPremiumTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void 괴리율은_업비트가를_바이낸스_원화_환산가로_나눈_것이다() {
        KimchiPremium p = KimchiPremium.of("KRW-BTC", 105_000_000, 72_463.77, 1_380.0, "dunamu", 1_700_000_000_000L);

        assertEquals(100_000_002.6, p.globalPriceKrw(), 1.0);
        assertEquals(5.0, p.premiumPercent(), 0.001);
        assertEquals(-2.0, KimchiPremium.premiumPercent(98, 100), 1e-9);
        assertTrue(p.updatedAtIso().endsWith("+09:00"));
    }

    @Test
    void 바이낸스_miniTicker_메시지에서_심볼과_종가를_뽑는다() {
        String json = "{\"stream\":\"btcusdt@miniTicker\",\"data\":{\"e\":\"24hrMiniTicker\",\"E\":1700000000123,"
                + "\"s\":\"BTCUSDT\",\"c\":\"72463.77\",\"o\":\"70000\",\"h\":\"73000\",\"l\":\"69000\",\"v\":\"1\",\"q\":\"1\"}}";

        Optional<Map.Entry<String, BinancePriceFeed.PriceAt>> parsed = BinancePriceFeed.parse(mapper, json);

        assertTrue(parsed.isPresent());
        assertEquals("BTCUSDT", parsed.get().getKey());
        assertEquals(72_463.77, parsed.get().getValue().price(), 1e-9);
        assertEquals(1_700_000_000_123L, parsed.get().getValue().timestamp());
        assertTrue(BinancePriceFeed.parse(mapper, "{\"stream\":\"x\",\"data\":{\"e\":\"trade\"}}").isEmpty());
        assertTrue(BinancePriceFeed.parse(mapper, "not json").isEmpty());
        assertEquals("DOGEUSDT", BinancePriceFeed.symbolFor("KRW-DOGE"));
    }

    @Test
    void 환율_응답_두_종류를_읽는다() {
        assertEquals(1_380.5, ExchangeRateFeed.parseDunamu(mapper,
                "[{\"code\":\"FRX.KRWUSD\",\"basePrice\":1380.5}]").getAsDouble(), 1e-9);
        assertEquals(1_379.0, ExchangeRateFeed.parseErApi(mapper,
                "{\"result\":\"success\",\"rates\":{\"KRW\":1379.0}}").getAsDouble(), 1e-9);
        assertTrue(ExchangeRateFeed.parseDunamu(mapper, "[]").isEmpty());
        assertTrue(ExchangeRateFeed.parseErApi(mapper, "oops").isEmpty());
    }

    @Test
    void 업비트_마지막_체결가는_더_최신_체결만_남긴다() {
        UpbitLastPriceStore store = new UpbitLastPriceStore(mapper);
        store.put("KRW-BTC", 100, 10);
        store.put("KRW-BTC", 90, 5);
        store.put("KRW-BTC", 110, 20);

        assertEquals(110, store.latest("KRW-BTC").orElseThrow().price(), 1e-9);
        assertTrue(store.latest("KRW-ETH").isEmpty());
    }
}
