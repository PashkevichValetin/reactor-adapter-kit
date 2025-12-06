package com.veldev.reactor_adapter_kit.controller;

import com.veldev.reactor_adapter_kit.model.StockData;
import com.veldev.reactor_adapter_kit.services.StockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
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
    @GetMapping(value = "/stream/{symbol}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
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
    @GetMapping("/price/{symbol}")
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

    //Получаем список доступных символов
    @GetMapping("/symbols")
    public Flux<String> getAvailableSymbols() {
        log.info("Requesting available symbols");
        return stockService.getAvailableSymbols();
    }

    // Следим за несколькими акциями одновременно (SSE)
    @GetMapping(value = "/watchlist", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<StockData>> watchlist(@RequestParam String[] symbols) {
        log.info("Watchlist requested with symbols: {}", (Object) symbols);

        return stockService.watchlist(symbols)
                .map(stockData -> ServerSentEvent.<StockData>builder()
                        .data(stockData)
                        .event("watchlist-update")
                        .build());
    }

    // Проверяем состояние сервиса
    @GetMapping("/health")
    public Mono<Map<String, Object>> healthCheck() {
        log.debug("Health check requested");

        return Mono.just(Map.of(
                "status", "UPP",
                "service", "Stock Service API",
                "timestamp", LocalDateTime.now(),
                "version", "1.0.0"
        ));
    }

    // Сравниваем две акции
    @GetMapping("/compare")
    public Mono<Map<String, Object>> compareStocks(
            @RequestParam String symbol1,
            @RequestParam String symbol2) {
        log.info("Comparing stocks: {} vs {}", symbol1, symbol2);

        return Mono.zip(
                stockService.currentPrice(symbol1)
                        .map(data -> createStockInfo(data))
                        .onErrorResume(e -> Mono.just(createErrorInfo(symbol1, e))),
                stockService.currentPrice(symbol2)
                        .map(data -> createStockInfo(data))
                        .onErrorResume(e -> Mono.just(createErrorInfo(symbol2, e)))
        ).map(tuple -> {
            Map<String, Object> stock1 = tuple.getT1();
            Map<String, Object> stock2 = tuple.getT2();

            Map<String, Object> comparison = new HashMap<>();
            comparison.put("stock1", stock1);
            comparison.put("stock2", stock2);
            comparison.put("timestamp", LocalDateTime.now());

            if (stock1.get("success").equals(true) && stock2.get("success").equals(true)) {
                double price1 = (double) stock1.get("price");
                double price2 = (double) stock2.get("price");
                comparison.put("priceDifference", price1 - price2);
                comparison.put("priceDifferencePercent", ((price1 - price2) / price2) * 100);
                comparison.put("comparisonSuccessful", true);
            } else {
                comparison.put("comparisonSuccessful", false);
            }
            return comparison;
        });

    }

    private Map<String, Object> createStockInfo(StockData data) {
        Map<String, Object> info = new HashMap<>();
        info.put("symbol", data.getSymbol());
        info.put("price", data.getPrice());
        info.put("change", data.getChange());
        info.put("changePercent", data.getChangePercent());
        info.put("timestamp", data.getTimestamp());
        info.put("success", true);
        return info;
    }

    private Map<String, Object> createErrorInfo(String symbol, Throwable error) {
        Map<String, Object> info = new HashMap<>();
        info.put("symbol", symbol);
        info.put("error", error);
        info.put("success", false);
        info.put("timestamp", LocalDateTime.now());
        return info;

    }
}
