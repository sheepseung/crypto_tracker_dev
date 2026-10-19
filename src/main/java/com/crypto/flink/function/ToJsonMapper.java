package com.crypto.flink.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 결과 객체를 JSON 문자열로 바꾼다. 우리 DTO 의 직렬화 실패는 버그이므로 삼키지 않고 Job 을 실패시킨다.
 */
public class ToJsonMapper<T> extends RichMapFunction<T, String> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public String map(T value) throws Exception {
        return objectMapper.writeValueAsString(value);
    }
}
