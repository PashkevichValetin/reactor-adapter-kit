package com.veldev.reactor_adapter_kit.controller;

import com.veldev.reactor_adapter_kit.model.StockData;
import com.veldev.reactor_adapter_kit.services.StockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.awt.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/stock")
@RequiredArgsConstructor
public class StockController {
    private final StockService stockService;

    // Получить поток обновлений по акции в реальном времени (SSE)
    @GetMapping(value = "/stream/{symbol)", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<StockData>> streamStock(@PathVariable String symbol) {
        log.info("Requesting stock for symbol: {}", symbol);

        return stockService.streamStock(symbol)
                .map(stockData -> ServerSentEvent.<StockData>builder()
                        .data(stockData)
                        .event("stock-update")
                        .build())
                .doOnError(error -> log.error("Stream error for {}: {}", symbol, error.getMessage()))
                .onErrorResume(e -> Flux.just(
                        ServerSentEvent.<StockData>builder()
                                .event("error")
                                .data(StockData.builder()
                                        .symbol(symbol)
                                        .build())
                                .build()
                ));
    }

    // Получаем текущую цену акции
    @GetMapping("/price/{symbol")
    public Mono<Map<String, Object>> getCurrentPrice(@PathVariable String symbol) {
        log.info("Requesting current price for symbol: {}", symbol);

        return stockService.currentPrice(symbol)
                .map(stockData -> {
                    Map<String, Object> response = new HashMap<>();
                    response.put("symbol", stockData.getSymbol());
                    response.put("price", stockData.getPrice());
                    response.put("timestamp", stockData.getTimestamp());
                    response.put("success", true);
                    return response;
                })
                .onErrorResume(e -> {
                    log.error("Failed to get price for {}: {}", symbol, e.getMessage());
                    Map<String, Object> errorResponse = new HashMap<>();
                    errorResponse.put("symbol", symbol);
                    errorResponse.put("error", e.getMessage());
                    errorResponse.put("success", false);
                    errorResponse.put("timestamp", LocalDateTime.now());
                    return Mono.just(errorResponse);
                });
    }
}
