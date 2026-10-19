package com.crypto.flink.rules;

/** 파이프라인이 Job 을 띄우는 시점의 규칙 스냅샷을 얻는 통로. 테스트에서는 고정값을 넣는다. */
@FunctionalInterface
public interface RulesProvider {

    DetectionRules current();
}
