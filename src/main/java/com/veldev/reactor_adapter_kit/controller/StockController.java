package com.veldev.reactor_adapter_kit.controller;

import com.veldev.reactor_adapter_kit.dto.StockComparisonResult;
import com.veldev.reactor_adapter_kit.dto.StockResponse;
import com.veldev.reactor_adapter_kit.mapper.StockMapper;
import com.veldev.reactor_adapter_kit.model.StockData;
import com.veldev.reactor_adapter_kit.services.StockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
public class StockController {
    private final StockService stockService;
    private final Map<String, AtomicInteger> eventCounterMap = new ConcurrentHashMap<>();

    @GetMapping(value = "/stream/{symbol}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<StockData>> streamStock(@PathVariable String symbol) {
        log.info("Requesting stock stream for symbol: {}", symbol);

        return Flux.defer(() -> {
            AtomicInteger counter = eventCounterMap.computeIfAbsent(symbol, k -> new AtomicInteger(0));

            return stockService.streamStock(symbol)
                    .map(stockData -> ServerSentEvent.<StockData>builder()
                            .data(stockData)
                            .event("stock-update")
                            .id(symbol + "-" + counter.getAndIncrement())
                            .build())
                    .doOnSubscribe(sub -> log.info("SSE stream started for {}", symbol))
                    .doOnError(error -> log.error("Stream error for {}", error.getMessage()))
                    .doOnTerminate(() -> log.info("SSE stream terminated for {}", symbol));
        });
    }

    @GetMapping("/price/{symbol}")
    public Mono<StockResponse> getCurrentPrice(@PathVariable String symbol) {
        log.info("Requesting current price for symbol: {}", symbol);

        return stockService.currentPrice(symbol)
                .map(StockResponse::from)
                .onErrorResume(e -> {
                    log.error("Failed to get price for {}: {}", symbol, e.getMessage());
                    return Mono.just(StockResponse.error(symbol, e.getMessage()));
                });
    }

    @GetMapping("/symbols")
    public Mono<Map<String, Object>> getAvailableSymbols() {
        log.info("Requesting available symbols");

        return stockService.getAvailableSymbols()
                .collectList()
                .map(symbols -> {
                    Map<String, Object> response = new HashMap<>();
                    response.put("symbols", symbols);
                    response.put("count", symbols.size());
                    response.put("timestamp", LocalDateTime.now());
                    return response;
                })
                .doOnSuccess(data -> log.info("Available symbols sent: {} symbols",
                        data.get("count")));
    }

    @GetMapping(value = "/watchlist", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<StockData>> watchlist(@RequestParam String[] symbols) {
        log.info("Watchlist requested with symbols: {}", (Object) symbols);

        return Flux.defer(() -> {
            log.info("Creating virtual watchlist stream for symbols: {}", symbols);

            return stockService.watchlist(symbols)
                    .map(stockData -> ServerSentEvent.<StockData>builder()
                            .data(stockData)
                            .event("watchlist_update")
                            .id(stockData.getSymbol() + "-" + UUID.randomUUID())
                            .build())
                    .doOnSubscribe(sub -> log.info("Watchlist stream started for {}", (Object) symbols))
                    .doOnError(error -> log.error("Watchlist stream error: {}", error.getMessage()))
                    .doOnTerminate(() -> log.info("Watchlist stream terminated for {}", (Object) symbols));
        });
    }

    @GetMapping("/health")
    public Mono<Map<String, Serializable>> healthCheck() {
        log.debug("Health check requested");

        return Mono.just(Map.of(
                "status", "UP",
                "service", "Stock Service API",
                "timestamp", LocalDateTime.now(),
                "version", "1.0.0"
        ));
    }

    @GetMapping("/compare")
    public Mono<StockComparisonResult> compareStocks(
            @RequestParam String symbol1,
            @RequestParam String symbol2) {
        log.info("Comparing stocks: {} vs {}", symbol1, symbol2);
        return Mono.zip(
                stockService.currentPrice(symbol1).map(StockMapper.INSTANCE::toStockResponse),
                stockService.currentPrice(symbol2).map(StockMapper.INSTANCE::toStockResponse)
        ).flatMap(tuple -> {
            StockResponse stock1 = tuple.getT1();
            StockResponse stock2 = tuple.getT2();

            StockComparisonResult comparison = new StockComparisonResult(
                    stock1,
                    stock2,
                    LocalDateTime.now(),
                    false,
                    null,
                    null,
                    "Failed to retrieve data from one or both symbols"
            );

            if (stock1.success() && stock2.success()) {
                BigDecimal price1 = stock1.price();
                BigDecimal price2 = stock2.price();
                BigDecimal diff = price1.subtract(price2);
                BigDecimal diffPercent = diff.divide(price1, 4, RoundingMode.HALF_UP)
                        .multiply(BigDecimal.valueOf(100));

                comparison = new StockComparisonResult(
                        stock1,
                        stock2,
                        LocalDateTime.now(),
                        true,
                        diff,
                        diffPercent,
                        null
                );
            }

            return Mono.just(comparison);
        }).onErrorResume(e -> {
            log.warn("Comparison failed: {}", e.getMessage());
            return Mono.just(new StockComparisonResult(
                    StockResponse.error(symbol1, e.getMessage()),
                    StockResponse.error(symbol2, e.getMessage()),
                    LocalDateTime.now(),
                    false,
                    null,
                    null,
                    e.getMessage()
            ));
        });
    }
}