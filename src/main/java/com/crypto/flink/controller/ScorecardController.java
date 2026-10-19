package com.crypto.flink.controller;

import com.crypto.flink.scorecard.ScorecardService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/api/scorecard")
@Tag(name = "Scorecard", description = "신호 성적표. 이벤트 뒤 5·15·60분 수익률(%)의 건수·평균·방향 적중률")
public class ScorecardController {

    private final ScorecardService service;

    public ScorecardController(ScorecardService service) {
        this.service = service;
    }

    @GetMapping
    @Operation(summary = "종류별(옵션: 종목별) 성적표", description = "days 는 1~90. byCode=true 면 종류×종목")
    public List<ScorecardService.Row> scorecard(
            @RequestParam(defaultValue = "7") int days,
            @RequestParam(defaultValue = "false") boolean byCode) {
        if (days < 1 || days > 90) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "days 는 1~90");
        }
        return service.scorecard(days, byCode);
    }
}
