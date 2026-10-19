package com.crypto.flink.context;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ContextServiceTest {

    @Test
    void 백분위는_표본_중_값_이하인_비율이고_같은_값은_모두_포함한다() {
        double[] samples = {10, 20, 20, 30, 40};
        assertEquals(20.0, ContextService.percentile(samples, 10), 1e-9);
        assertEquals(60.0, ContextService.percentile(samples, 20), 1e-9);
        assertEquals(60.0, ContextService.percentile(samples, 25), 1e-9);
        assertEquals(100.0, ContextService.percentile(samples, 40), 1e-9);
        assertEquals(100.0, ContextService.percentile(samples, 99), 1e-9);
        assertEquals(0.0, ContextService.percentile(samples, 1), 1e-9);
    }
}
