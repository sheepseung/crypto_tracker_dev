package com.crypto.flink.controller;

import com.crypto.flink.orderbook.OrderbookImbalance;
import com.crypto.flink.orderbook.OrderbookService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/orderbook")
@Tag(name = "Orderbook", description = "종목별 호가 쏠림. ratio = 총 매수 잔량 ÷ 총 매도 잔량")
public class OrderbookController {

    private final OrderbookService service;

    public OrderbookController(OrderbookService service) {
        this.service = service;
    }

    @GetMapping
    @Operation(summary = "종목별 호가 쏠림 최신 스냅샷")
    public List<OrderbookImbalance> current() {
        return service.current();
    }
}
