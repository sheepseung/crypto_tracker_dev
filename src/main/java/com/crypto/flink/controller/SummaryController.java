package com.crypto.flink.controller;

import com.crypto.flink.summary.DailySummary;
import com.crypto.flink.summary.DailySummaryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

@RestController
@RequestMapping("/api/summary")
@Tag(name = "Daily Summary", description = "하루(KST) 체결 금액·이벤트 요약. 5분 버킷과 이벤트 이력에서 계산")
public class SummaryController {

    private final DailySummaryService service;

    public SummaryController(DailySummaryService service) {
        this.service = service;
    }

    @GetMapping("/daily")
    @Operation(summary = "일별 요약", description = "date 는 YYYY-MM-DD (KST). 비우면 오늘")
    public DailySummary daily(@RequestParam(required = false) String date) {
        if (date == null) {
            return service.today();
        }
        try {
            return service.forDate(LocalDate.parse(date));
        } catch (DateTimeParseException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "date 는 YYYY-MM-DD: " + date);
        }
    }
}
