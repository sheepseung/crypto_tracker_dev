package com.crypto.flink.function;

import com.crypto.flink.dto.TradeIntensity;
import com.crypto.flink.util.KstTime;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 누적 결과에 종목 코드와 실제 윈도우 시간을 붙여 TradeIntensity 로 만든다.
 */
public class IntensityWindowFunction
        extends ProcessWindowFunction<TradeAmountAggregator.Accumulator, TradeIntensity, String, TimeWindow> {

    @Override
    public void process(String code, Context context, Iterable<TradeAmountAggregator.Accumulator> accumulators,
            Collector<TradeIntensity> out) {
        // AggregateFunction 과 함께 쓰면 누적기 하나만 넘어온다
        TradeAmountAggregator.Accumulator acc = accumulators.iterator().next();
        TimeWindow window = context.window();

        TradeIntensity result = new TradeIntensity();
        result.setCode(code);
        result.setWindowSeconds((int) ((window.getEnd() - window.getStart()) / 1000));
        result.setWindowStart(window.getStart());
        result.setWindowEnd(window.getEnd());
        result.setWindowEndAt(KstTime.format(window.getEnd()));
        result.setBuyAmount(acc.buyAmount);
        result.setSellAmount(acc.sellAmount);
        result.setTotalAmount(acc.buyAmount + acc.sellAmount);
        result.setNetAmount(acc.buyAmount - acc.sellAmount);
        result.setBuyVolume(acc.buyVolume);
        result.setSellVolume(acc.sellVolume);
        result.setTradeCount(acc.tradeCount);
        result.setIntensity(TradeIntensity.intensityOf(acc.buyAmount, acc.sellAmount));
        result.setBuyRatio(TradeIntensity.buyRatioOf(acc.buyAmount, acc.sellAmount));
        result.setLastTradeTimestamp(acc.lastTradeTimestamp);
        out.collect(result);
    }
}
