package com.veldev.reactor_adapter_kit.services;

import com.veldev.reactor_adapter_kit.model.StockData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.*;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class ReactiveStockService implements StockService {

    private static final List<String> SUPPORTED_SYMBOLS = Arrays.asList(
            "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
            "META", "NVDA", "NFLX", "BTC-USD", "ETH-USD"
    );

    private final Random random = new Random();
    private final Map<String, Double> basePrices = new ConcurrentHashMap<>();
    
    // Реактивный sink для отправки обновлений акций
    private final Sinks.Many<StockData> stockUpdatesSink = 
            Sinks.many().multicast().onBackpressureBuffer(1000);
    
    // Публичный поток обновлений акций
    private final Flux<StockData> marketDataStream;
    
    public ReactiveStockService() {
        log.info("Initializing ReactiveStockService...");
        
        // Инициализация базовых цен
        initializeBasePrices();
        
        // Создаем реактивный поток рыночных данных
        this.marketDataStream = createMarketDataStream();
        
        log.info("ReactiveStockService initialized with {} symbols", SUPPORTED_SYMBOLS.size());
    }
    
    private void initializeBasePrices() {
        SUPPORTED_SYMBOLS.forEach(symbol -> 
            basePrices.put(symbol, 100.0 + random.nextDouble() * 900.0)
        );
    }
    
    private Flux<StockData> createMarketDataStream() {
        return Flux.interval(Duration.ofSeconds(1))
                .flatMap(tick -> generateMarketDataUpdates())
                .doOnNext(stockData -> {
                    // Эмитим обновления в sink для подписчиков
                    stockUpdatesSink.tryEmitNext(stockData);
                })
                .publish()
                .autoConnect(0) // Начинаем эмитить сразу
                .doOnSubscribe(sub -> log.info("Market data stream started"))
                .doOnError(error -> log.error("Market data stream error: {}", error.getMessage()))
                .retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)))
                .subscribeOn(Schedulers.boundedElastic());
    }
    
    private Flux<StockData> generateMarketDataUpdates() {
        return Flux.fromIterable(SUPPORTED_SYMBOLS)
                .flatMap(symbol -> 
                    Mono.fromCallable(() -> generateStockData(symbol))
                        .subscribeOn(Schedulers.boundedElastic())
                )
                .doOnNext(data -> log.debug("Generated: {} = ${}", data.getSymbol(), data.getPrice()));
    }

    @Override
    public Flux<StockData> streamStock(String symbol) {
        if (symbol == null || symbol.trim().isEmpty()) {
            return Flux.error(new IllegalArgumentException("Symbol cannot be null or empty"));
        }

        String normalizedSymbol = symbol.trim().toUpperCase();

        if (!isSymbolSupported(normalizedSymbol)) {
            return Flux.error(new IllegalArgumentException("Unsupported symbol: " + normalizedSymbol));
        }

        log.info("Starting reactive stoc stream for: {}", normalizedSymbol);

        return stockUpdatesSink.asFlux()
            .filter(stockData -> stockData.getSymbol().equals(normalizedSymbol))
            .doOnSubscribe(sub -> log.info("Client subscribed to {} stream", normalizedSymbol))
            .doOnCancel(() -> log.info("{} stream canceled by client", normalizedSymbol))
            .doOnError(error -> log.error("Stream error for {}: {}", normalizedSymbol, error.getMessage()))
            .onErrorResume(error -> {
                log.warn("Recovering {} stream after error: {}", normalizedSymbol, error.getMessage());
                return Flux.empty();
            });
    }

    @Override
    public Mono<StockData> currentPrice(String symbol) {
        if (symbol == null || symbol.trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Smbol cannot be null or empty"));
        }

        String normalizedSymbol = symbol.trim().toUpperCase();

                if (!isSymbolSupported(normalizedSymbol)) {
            return Mono.error(new IllegalArgumentException("Unsupported symbol: " + normalizedSymbol));
        }

        log.info("Fetching current price for: {}", normalizedSymbol);

        return Mono.fromCallable(() -> generateStockData(normalizedSymbol))
            .subscribeOn(Schedulers.boundedElastic())
            .timeout(Duration.ofSeconds(3))
            .doOnSuccess(data -> 
                log.info("Successfully fetched price for {}: ${}", normalizedSymbol, data.getPrice())
            )
            .doOnError(error -> 
                log.error("Failed to fetch price for {}: {}", normalizedSymbol, error.getMessage())
            )
            .onErrorResume(error -> 
                Mono.error(new IllegalArgumentException("Failed to get current price for " + normalizedSymbol, error))
            );
    }

    @Override
    public Flux<String> getAvailableSymbols() {
        log.info("Fetching available symbols");

        return Flux.fromIterable(SUPPORTED_SYMBOLS)
            .sort()
            .doOnSubscribe(sub -> log.debug("Started symbols stream"))
            .doOnComplete(() -> log.debug("Symbols stream completed"));
    }

    @Override
    public Flux<StockData> watchlist(String... symbols) {
        if (symbols == null || symbols.length == 0) {
            return Flux.error(new IllegalArgumentException("Symbols list cannot be null or empty"));
        }

        log.info("Creating watchlist for symbols: {}", Arrays.toString(symbols));

        return Flux.fromArray(symbols)
        .filter(symbol -> symbol != null && !symbol.trim().isEmpty())
        .map(String::trim)
        .map(String::toUpperCase)
        .distinct()
        .filter(this::isSymbolSupported)
        .flatMap(this::streamStock)
        .doOnSubscribe(sub -> log.info("Watchlist stream started"))
        .doOnError(error -> log.error("Watchlist error: {}", error.getMessage()));
    }

    // Дополнительные реактивные методы

    // Получаем поток всех рыночных данных
    public Flux<StockData> getMarketDataStream() {
        return marketDataStream;
    }

    // Подписываемся на обновления символов фильтрации
   public Flux<StockData> subscribeToSymbols(List<String> symbols) {
        return stockUpdatesSink.asFlux()
                .filter(stockData -> symbols.contains(stockData.getSymbol()))
                .doOnSubscribe(sub -> log.info("Subscribed to symbols: {}", symbols))
                .doOnCancel(() -> log.info("Unsubscribed from symbols: {}", symbols));
    }
    
    // Генерируем тестовых данных акции
     
    private StockData generateStockData(String symbol) {
        double basePrice = basePrices.getOrDefault(symbol, 100.0);
        
        // Имитация рыночных колебаний
        double change = (random.nextDouble() - 0.5) * 10;
        double newPrice = Math.max(0.1, basePrice + change);
        
        // Обновляем базовую цену
        basePrices.put(symbol, newPrice);
        
        double changePercent = (change / basePrice) * 100;
        
        return StockData.builder()
                .symbol(symbol)
                .price(round(newPrice, 2))
                .change(round(change, 2))
                .changePercent(round(changePercent, 3))
                .volume(1_000_000L + random.nextInt(9_000_000))
                .timestamp(LocalDateTime.now())
                .build();
    }
    
    private boolean isSymbolSupported(String symbol) {
        return SUPPORTED_SYMBOLS.contains(symbol);
    }
    
    private double round(double value, int places) {
        double scale = Math.pow(10, places);
        return Math.round(value * scale) / scale;
    }
    
    // Метод для ручной эмитации обновления акции 
     
    public void emitStockUpdate(StockData stockData) {
        stockUpdatesSink.tryEmitNext(stockData);
    }
    
    // Получаем статистику сервиса
     
   public Mono<Map<String, Object>> getServiceStats() {
    return Mono.fromCallable(() -> {
        Map<String, Object> stats = new HashMap<>();
        stats.put("supportedSymbols", SUPPORTED_SYMBOLS.size());
        stats.put("basePricesInitialized", basePrices.size());
        stats.put("timestamp", LocalDateTime.now());
        return stats;
        });
    }

}
