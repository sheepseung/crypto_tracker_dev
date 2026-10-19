package com.crypto.flink.controller;

import com.crypto.flink.dto.DetectionEvent;
import com.crypto.flink.store.DetectionEventRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;

@RestController
@RequestMapping("/api/events")
@Tag(name = "Detection Events", description = "대량 체결·급등락 감지 이벤트 이력")
public class EventsController {

    private static final int MAX_LIMIT = 500;

    private final DetectionEventRepository repository;

    public EventsController(DetectionEventRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    @Operation(summary = "감지 이벤트 조회 (최신순)",
            description = "since 는 ISO-8601(예: 2024-06-10T15:00:00+09:00) 또는 epoch ms. 기본은 최근 24시간")
    public List<DetectionEvent> find(
            @RequestParam(required = false) String code,
            @RequestParam(required = false) String type,
            @RequestParam(required = false) String since,
            @RequestParam(defaultValue = "50") int limit) {
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit 은 1~" + MAX_LIMIT);
        }
        long sinceMillis = since == null
                ? Instant.now().minus(Duration.ofHours(24)).toEpochMilli()
                : parseSince(since);
        return repository.find(
                code == null ? null : code.toUpperCase(),
                type == null ? null : type.toUpperCase(),
                sinceMillis,
                limit);
    }

    static long parseSince(String since) {
        if (since.chars().allMatch(Character::isDigit)) {
            return Long.parseLong(since);
        }
        try {
            return Instant.from(java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(since)).toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "since 형식이 잘못됨: " + since);
        }
    }
}
