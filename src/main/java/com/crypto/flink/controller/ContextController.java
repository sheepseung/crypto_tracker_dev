package com.crypto.flink.controller;

import com.crypto.flink.context.ContextService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/context")
@Tag(name = "Context", description = "평소 대비 백분위. 종목별 최근 7일 5분 버킷 분포에서 지금 5분 값의 위치(0~100)")
public class ContextController {

    private final ContextService service;

    public ContextController(ContextService service) {
        this.service = service;
    }

    @GetMapping
    @Operation(summary = "종목별 체결강도·거래대금·순매수 백분위", description = "분포가 아직 없으면 null")
    public List<ContextService.CodeContext> current() {
        return service.current();
    }
}
