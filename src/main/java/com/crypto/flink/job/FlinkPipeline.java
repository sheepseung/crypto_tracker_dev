package com.crypto.flink.job;

import com.crypto.flink.dto.CryptoTrade;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 파싱이 끝난 체결 스트림 위에 자기 연산과 싱크를 붙이는 단위.
 * 실행은 FlinkJobRunner 가 한 번만 하므로 구현체는 execute() 를 부르지 않는다.
 */
public interface FlinkPipeline {

    String name();

    boolean isEnabled();

    void attach(DataStream<CryptoTrade> trades);
}
