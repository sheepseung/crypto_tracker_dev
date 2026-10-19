package com.crypto.flink.function;

import com.crypto.flink.dto.CryptoTrade;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class TradeJsonParserTest {

    @Test
    void 필수_필드가_다_있으면_통과한다() {
        assertNull(TradeJsonParser.validate(valid()));
    }

    @Test
    void trade_가_아닌_메시지는_거른다() {
        CryptoTrade ticker = valid();
        ticker.setType("ticker");
        assertNotNull(TradeJsonParser.validate(ticker));
    }

    @Test
    void ask_bid_가_이상하면_거른다() {
        CryptoTrade trade = valid();
        trade.setAskBid("BUY");
        assertNotNull(TradeJsonParser.validate(trade));
    }

    @Test
    void 가격이나_수량이_0_이하면_거른다() {
        CryptoTrade zeroVolume = valid();
        zeroVolume.setTradeVolume(0.0);
        assertNotNull(TradeJsonParser.validate(zeroVolume));

        CryptoTrade noPrice = valid();
        noPrice.setTradePrice(null);
        assertNotNull(TradeJsonParser.validate(noPrice));
    }

    @Test
    void 빈_메시지는_거른다() {
        assertEquals("빈 메시지", TradeJsonParser.validate(null));
    }

    private static CryptoTrade valid() {
        CryptoTrade trade = new CryptoTrade();
        trade.setType(CryptoTrade.TYPE_TRADE);
        trade.setCode("KRW-ETH");
        trade.setAskBid(CryptoTrade.ASK);
        trade.setTradePrice(4_000_000.0);
        trade.setTradeVolume(1.5);
        trade.setTradeTimestamp(1_700_000_000_000L);
        return trade;
    }
}
