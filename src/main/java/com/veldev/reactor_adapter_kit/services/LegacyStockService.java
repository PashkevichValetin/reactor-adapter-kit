package com.veldev.reactor_adapter_kit.services;

import com.veldev.reactor_adapter_kit.model.StockData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class LegacyStockService {

    private final Random random = new Random();
    private final ConcurrentHashMap<String, StockCallback> subscriptions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final String[] availableSymbols = {
            "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
            "META", "NVDA", "NFLX", "BTC-USD", "ETH-USD"
    };

    private final ConcurrentHashMap<String, Double> basePrices = new ConcurrentHashMap<>();

    public LegacyStockService() {
        for (String symbol : availableSymbols) {
            basePrices.put(symbol, 100 + random.nextDouble() * 900);
        }

        startMarketSimulator();
        log.info("LegacyStockService initialized with {} symbols", availableSymbols.length);
    }

    public void subscribeToStock(String symbol, StockCallback callback) {
        if (symbol == null || symbol.trim().isEmpty()) {
            log.warn("Symbol cannot be null or empty");
            callback.onError(new IllegalArgumentException("Symbol cannot be null or empty"));
            return;
        }

        if (!isSymbolSupported(symbol)) {
            log.warn("Symbol {} not supported", symbol);
            callback.onError(new IllegalArgumentException("Unsupported symbol: " + symbol));
            return;
        }

        log.info("Subscribing to stock: {}", symbol);

        // Проверка существующей подписки
        StockCallback existing = subscriptions.putIfAbsent(symbol, callback);
        if (existing != null) {
            log.warn("Symbol {} already subscribed", symbol);
            callback.onError(new IllegalStateException("Already subscribed to: " + symbol));
            return;
        }

        scheduler.schedule(() -> {
            StockData initialData = generateStockData(symbol);
            callback.onUpdate(initialData);
        }, 100, TimeUnit.MILLISECONDS);
    }

    public void unsubscribeFromStock(String symbol) {
        if (symbol == null || symbol.trim().isEmpty()) {
            log.warn("Cannot unsubscribe from null symbol");
            return;
        }

        log.info("Unsubscribing from stock: {}", symbol);
        StockCallback removed = subscriptions.remove(symbol);
        if (removed == null) {
            log.debug("No active subscription found for: {}", symbol);
        }
    }

    public void getCurrentPrice(String symbol, StockCallback callback) {
        if (symbol == null || symbol.trim().isEmpty()) {
            log.warn("Symbol cannot be null or empty");
            callback.onError(new IllegalArgumentException("Symbol cannot be null or empty"));
            return;
        }

        log.info("Fetching current price for: {}", symbol);

        scheduler.schedule(() -> {
            if (random.nextDouble() < 0.1) {
                callback.onError(new RuntimeException("Market data temporarily unavailable"));
            } else {
                StockData data = generateStockData(symbol);
                callback.onUpdate(data);
            }
        }, 300 + random.nextInt(700), TimeUnit.MILLISECONDS);
    }

    public boolean isSymbolSupported(String symbol) {
        if (symbol == null) {
            return false;
        }

        for (String s : availableSymbols) {
            if (s.equalsIgnoreCase(symbol)) {
                return true;
            }
        }
        return false;
    }

    public String[] getAvailableSymbols() {
        return availableSymbols.clone();
    }

    private StockData generateStockData(String symbol) {
        double basePrice = basePrices.getOrDefault(symbol, 100.0);
        double change = (random.nextDouble() - 0.5) * 10;
        double newPrice = Math.max(0.1, basePrice + change);

        basePrices.put(symbol, newPrice);

        return StockData.builder()
                .symbol(symbol)
                .price(round(newPrice, 2))
                .change(round(change, 2))
                .changePercent(round((change / basePrice) * 100, 3))
                .volume(1000000L + random.nextInt(9000000))
                .timestamp(LocalDateTime.now())
                .build();
    }

    private double round(double value, int places) {
        double scale = Math.pow(10, places);
        return Math.round(value * scale) / scale;
    }

    private void startMarketSimulator() {
        scheduler.scheduleAtFixedRate(() -> {
            for (String symbol : subscriptions.keySet()) {
                StockCallback callback = subscriptions.get(symbol);
                if (callback != null && random.nextDouble() > 0.3) {
                    StockData data = generateStockData(symbol);
                    try {
                        callback.onUpdate(data);
                    } catch (Exception e) {
                        log.error("Error sending update for {}: {}", symbol, e.getMessage());
                        // Удаляем проблемную подписку
                        subscriptions.remove(symbol);
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void cleanup() {
        log.info("Cleaning up LegacyStockService resources");

        // Очищаем все подписки
        subscriptions.clear();

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Forcing scheduler shutdown");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Cleanup interrupted: {}", e.getMessage());
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("LegacyStockService cleanup completed");
    }

    public int getActiveSubscriptionsCount() {
        return subscriptions.size();
    }
}