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
import reactor.util.retry.Retry;

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

        String normalizedSymbol = symbol.trim().toUpperCase();
        log.info("Starting stock stream for symbol: {}", normalizedSymbol);

        return Flux.<StockData>create(sink -> {
                    StockCallback callback = new StockCallback() {
                        @Override
                        public void onUpdate(StockData data) {
                            log.debug("Stock update: {} = ${}", data.getSymbol(), data.getPrice());
                            sink.next(data);
                        }

                        @Override
                        public void onError(Throwable error) {
                            log.error("Error in {} stream: {}", normalizedSymbol, error.getMessage());
                            sink.error(error);
                        }
                    };

                    legacyStockService.subscribeToStock(normalizedSymbol, callback);
                    log.info("Subscribed to {}", normalizedSymbol);

                    sink.onCancel(() -> {
                        log.info("Stream cancelled for: {}", normalizedSymbol);
                        legacyStockService.unsubscribeFromStock(normalizedSymbol);
                    });

                    sink.onDispose(() -> {
                        log.info("Stream disposed for: {}", normalizedSymbol);
                        legacyStockService.unsubscribeFromStock(normalizedSymbol);
                    });
                })
                .doOnSubscribe(sub -> log.info("Client subscribed to {} stream", normalizedSymbol))
                .doOnCancel(() -> log.info("{} stream cancelled", normalizedSymbol))
                .doOnError(error -> log.error("Error in {} stream: {}", normalizedSymbol, error.getMessage()))
                .publishOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<StockData> currentPrice(String symbol) {
        // Валидация входных параметров
        if (symbol == null || symbol.trim().isEmpty()) {
            log.warn("Empty symbol requested");
            return Mono.error(new IllegalArgumentException("Symbol cannot be null or empty"));
        }

        String normalizedSymbol = symbol.trim().toUpperCase();
        log.info("Fetching current price for: {}", normalizedSymbol);

        return Mono.<StockData>create(sink -> {
                    StockCallback callback = new StockCallback() {
                        @Override
                        public void onUpdate(StockData data) {
                            log.debug("Current price for {}: ${}", normalizedSymbol, data.getPrice());
                            sink.success(data);
                        }

                        @Override
                        public void onError(Throwable error) {
                            log.error("Error fetching price for {}: {}", normalizedSymbol, error.getMessage());
                            sink.error(error);
                        }
                    };

                    legacyStockService.getCurrentPrice(normalizedSymbol, callback);
                })
                .timeout(Duration.ofSeconds(5))
                .retryWhen(Retry.backoff(2, Duration.ofMillis(100)))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnSuccess(data -> {
                    log.info("Successfully fetched price for {}: ${}", normalizedSymbol, data.getPrice());
                })
                .doOnError(error -> {
                    log.error("Failed to get price for {}: {}", normalizedSymbol, error.getMessage());
                });
    }

    @Override
    public Flux<String> getAvailableSymbols() {
        return Flux.fromIterable(Arrays.asList(legacyStockService.getAvailableSymbols()))
                .sort()
                .doOnSubscribe(sub -> log.info("Fetching available symbols"))
                .doOnComplete(() -> log.info("Available symbols fetched successfully"));
    }

    @Override
    public Flux<StockData> watchlist(String... symbols) {
        // Валидация входных параметров
        if (symbols == null || symbols.length == 0) {
            return Flux.error(new IllegalArgumentException("Symbols list cannot be null or empty"));
        }

        log.info("Watchlist requested: {}", Arrays.toString(symbols));

        return Flux.fromArray(symbols)
                .filter(symbol -> symbol != null && !symbol.trim().isEmpty())
                .map(String::trim)
                .map(String::toUpperCase)
                .distinct()
                .flatMap(this::streamStock)
                .doOnSubscribe(sub -> log.info("Watching {} unique symbols", symbols.length))
                .doOnError(error -> log.error("Watchlist error: {}", error.getMessage()));
    }
}