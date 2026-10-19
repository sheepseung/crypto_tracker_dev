package com.crypto.flink.controller;

import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventsControllerTest {

    @Test
    void since_는_epoch_ms_와_ISO_offset_둘_다_받는다() {
        assertEquals(1_718_000_000_000L, EventsController.parseSince("1718000000000"));
        // 2024-06-10T15:20:00+09:00 = 06:20:00Z = 1718000000(06:13:20Z) + 400초
        assertEquals(1_718_000_400_000L, EventsController.parseSince("2024-06-10T15:20:00+09:00"));
    }

    @Test
    void since_형식이_틀리면_400() {
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> EventsController.parseSince("2024-06-10"));
        assertEquals(400, e.getStatusCode().value());
    }
}
