package com.crypto.flink.orderbook;

import com.crypto.flink.util.KstTime;

/**
 * 한 종목의 호가 쏠림 스냅샷.
 * ratio = 총 매수 잔량 ÷ 총 매도 잔량 (수량 기준, 1보다 크면 매수벽). topN 금액은 최우선 N호가의 잔량 × 가격(KRW).
 */
public record OrderbookImbalance(
        String code,
        double totalBidSize,
        double totalAskSize,
        double ratio,
        double bestBid,
        double bestAsk,
        double spreadPercent,
        double topBidAmount,
        double topAskAmount,
        int topLevels,
        long timestamp,
        String timestampIso) {

    // 매도 잔량이 0이면 비율이 무한대라 이 값으로 막는다
    public static final double MAX_RATIO = 999.99;

    public static OrderbookImbalance of(Orderbook book, int topLevels) {
        double ratio = ratioOf(book.totalBidSize, book.totalAskSize);
        double bestBid = book.units.isEmpty() ? 0 : book.units.get(0).bidPrice;
        double bestAsk = book.units.isEmpty() ? 0 : book.units.get(0).askPrice;
        double spread = bestBid > 0 ? (bestAsk - bestBid) / bestBid * 100.0 : 0;
        double topBid = 0;
        double topAsk = 0;
        for (int i = 0; i < Math.min(topLevels, book.units.size()); i++) {
            Orderbook.Unit unit = book.units.get(i);
            topBid += unit.bidPrice * unit.bidSize;
            topAsk += unit.askPrice * unit.askSize;
        }
        return new OrderbookImbalance(book.code, book.totalBidSize, book.totalAskSize, ratio, bestBid, bestAsk,
                spread, topBid, topAsk, topLevels, book.timestamp, KstTime.format(book.timestamp));
    }

    public static double ratioOf(double bidSize, double askSize) {
        if (askSize <= 0) {
            return bidSize <= 0 ? 1.0 : MAX_RATIO;
        }
        return Math.min(bidSize / askSize, MAX_RATIO);
    }
}
