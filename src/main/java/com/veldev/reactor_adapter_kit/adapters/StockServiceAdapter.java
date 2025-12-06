package com.veldev.reactor_adapter_kit.adapters;

import com.veldev.reactor_adapter_kit.model.StockData;
import com.veldev.reactor_adapter_kit.services.LegacyStockService;
import com.veldev.reactor_adapter_kit.services.StockCallback;
import com.veldev.reactor_adapter_kit.services.StockService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Arrays;

@Slf4j
@Component
public class StockServiceAdapter implements StockService {

    private final LegacyStockService legacyStockService;

    public StockServiceAdapter(LegacyStockService legacyStockService) {
        this.legacyStockService = legacyStockService;
        log.info("StockServiceAdapter initialized");
    }

    @Override
    public Flux<StockData> streamStock(String symbol) {
        // Валидация входных параметров
        if (symbol == null || symbol.trim().isEmpty()) {
            return Flux.error(new IllegalArgumentException("Symbol cannot be null or empty"));
        }

        log.info("Starting stock stream for symbol: {}", symbol);

        return Flux.<StockData>create(sink -> {
                    StockCallback callback = new StockCallback() {
                        @Override
                        public void onUpdate(StockData data) {
                            log.debug("Stock update: {} = ${}", data.getSymbol(), data.getPrice());
                            sink.next(data);
                        }

                        @Override
                        public void onError(Throwable error) {
                            log.error("Error in {} stream: {}", symbol, error.getMessage());
                            sink.error(error);
                        }
                    };

                    legacyStockService.subscribeToStock(symbol, callback);
                    log.info("Subscribed to {}", symbol);

                    sink.onCancel(() -> {
                        log.info("Stream cancelled for: {}", symbol);
                        legacyStockService.unsubscribeFromStock(symbol);
                    });

                    sink.onDispose(() -> {
                        log.info("Stream disposed for: {}", symbol);
                        legacyStockService.unsubscribeFromStock(symbol);
                    });
                })
                .doOnSubscribe(sub -> log.info("Client subscribed to {} stream", symbol))
                .doOnCancel(() -> log.info("{} stream cancelled", symbol))
                .doOnError(error -> log.error("Error in {} stream: {}", symbol, error.getMessage()));
    }

    @Override
    public Mono<StockData> currentPrice(String symbol) {
        // Валидация входных параметров
        if (symbol == null || symbol.trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Symbol cannot be null or empty"));
        }

        log.info("Fetching current price for: {}", symbol);

        return Mono.<StockData>create(sink -> {
                    StockCallback callback = new StockCallback() {
                        @Override
                        public void onUpdate(StockData data) {
                            log.debug("Current price for {}: ${}", symbol, data.getPrice());
                            sink.success(data);
                        }

                        @Override
                        public void onError(Throwable error) {
                            log.error("Error fetching price for {}: {}", symbol, error.getMessage());
                            sink.error(error);
                        }
                    };

                    legacyStockService.getCurrentPrice(symbol, callback);
                })
                .timeout(Duration.ofSeconds(3))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnSuccess(data -> {
                    log.info("Successfully fetched price for {}: ${}", symbol, data.getPrice());
                })
                .doOnError(error -> {
                    log.error("Failed to get price for {}: {}", symbol, error.getMessage());
                });
    }

    @Override
    public Flux<String> getAvailableSymbols() {
        return Flux.fromIterable(Arrays.asList(legacyStockService.getAvailableSymbols()))
                .sort()
                .doOnSubscribe(sub -> log.info("Fetching available symbols"));
    }

    public Flux<StockData> watchlist(String... symbols) {
        // Валидация входных параметров
        if (symbols == null || symbols.length == 0) {
            return Flux.error(new IllegalArgumentException("Symbols list cannot be null or empty"));
        }

        log.info("Watchlist requested: {}", Arrays.toString(symbols));

        return Flux.fromArray(symbols)
                .flatMap(symbol -> {
                    if (symbol == null || symbol.trim().isEmpty()) {
                        log.warn("Empty symbol skipped in watchlist");
                        return Flux.empty();
                    }
                    return streamStock(symbol)
                            .onErrorResume(e -> {
                                log.warn("Stream error for {}: {}", symbol, e.getMessage());
                                // Создаем объект с ошибкой вместо полного игнорирования
                                return Flux.just(StockData.builder()
                                        .symbol(symbol)
                                        .price(0.0)
                                        .change(0.0)
                                        .changePercent(0.0)
                                        .volume(0L)
                                        .timestamp(java.time.LocalDateTime.now())
                                        .build());
                            });
                })
                .doOnSubscribe(sub -> log.info("Watching {} symbols", symbols.length))
                .doOnError(error -> log.error("Watchlist error: {}", error.getMessage()));
    }
}