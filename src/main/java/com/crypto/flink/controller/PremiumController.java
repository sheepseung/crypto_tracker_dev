package com.crypto.flink.controller;

import com.crypto.flink.premium.KimchiPremium;
import com.crypto.flink.premium.KimchiPremiumService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/premium")
@Tag(name = "Kimchi Premium", description = "업비트가 ÷ (바이낸스 USDT가 × USD/KRW) − 1. 5초마다 갱신")
public class PremiumController {

    private final KimchiPremiumService service;

    public PremiumController(KimchiPremiumService service) {
        this.service = service;
    }

    @GetMapping
    @Operation(summary = "종목별 김치 프리미엄 최신 값", description = "한쪽 가격이 10분 넘게 안 오면 그 종목은 빠진다")
    public List<KimchiPremium> current() {
        return service.current();
    }
}
