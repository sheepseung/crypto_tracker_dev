package com.crypto.flink.premium;

import com.crypto.flink.util.KstTime;

/**
 * 한 종목의 김치 프리미엄 스냅샷. 모든 가격은 KRW.
 * premiumPercent = (업비트가 ÷ (바이낸스 USDT가 × USD/KRW) − 1) × 100
 */
public record KimchiPremium(
        String code,
        double upbitPrice,
        double binancePriceUsdt,
        double usdKrw,
        double globalPriceKrw,
        double premiumPercent,
        String fxSource,
        long updatedAt,
        String updatedAtIso) {

    public static KimchiPremium of(String code, double upbitPrice, double binancePriceUsdt, double usdKrw,
            String fxSource, long updatedAt) {
        double globalKrw = binancePriceUsdt * usdKrw;
        double premium = premiumPercent(upbitPrice, globalKrw);
        return new KimchiPremium(code, upbitPrice, binancePriceUsdt, usdKrw, globalKrw, premium, fxSource,
                updatedAt, KstTime.format(updatedAt));
    }

    public static double premiumPercent(double upbitPriceKrw, double globalPriceKrw) {
        return (upbitPriceKrw / globalPriceKrw - 1.0) * 100.0;
    }
}
