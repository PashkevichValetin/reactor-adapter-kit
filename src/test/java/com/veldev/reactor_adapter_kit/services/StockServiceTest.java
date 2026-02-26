package com.veldev.reactor_adapter_kit.services;

import com.veldev.reactor_adapter_kit.model.StockData;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
public class StockServiceTest {

    @Autowired
    private ReactiveStockService stockService;

    @Test
    void currentPrice_ShouldReturnValidStockData() {
        // WHEN
        Mono<StockData> result = stockService.currentPrice("AAPL");

        // THEN
        StepVerifier.create(result)
                .assertNext(stockData -> {
                    assertEquals("AAPL", stockData.getSymbol());
                    assertNotNull(stockData.getPrice());
                    assertTrue(stockData.getPrice().compareTo(BigDecimal.ZERO) > 0);
                    assertNotNull(stockData.getChange());
                    assertNotNull(stockData.getChangePercent());
                    assertNotNull(stockData.getVolume());
                    assertNotNull(stockData.getTimestamp());
                })
                .verifyComplete();
    }

    @Test
    void currentPrice_WithInvalidSymbol_ShouldReturnError() {
        // WHEN
        Mono<StockData> result = stockService.currentPrice("INVALID");

        // THEN
        StepVerifier.create(result)
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void getAvailableSymbol_ShouldReturnNonEmptyList() {
        // WHEN
        Flux<String> symbols = stockService.getAvailableSymbols();

        // THEN
        StepVerifier.create(symbols.collectList())
                .assertNext(list -> {
                    assertFalse(list.isEmpty());
                    assertTrue(list.contains("AAPL"));
                    assertTrue(list.contains("GOOGL"));
                    assertTrue(list.contains("MSFT"));
                    assertTrue(list.contains("TSLA"));
                })
                .verifyComplete();
    }

    @Test
    void streamStock_ShouldProvideContinuousUpdates() {
        // WHEN
        Flux<StockData> stream = stockService.streamStock("AAPL").take(3);

        // THEN
        StepVerifier.create(stream)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    void watchlistShouldProviderUpdatesForMultipleSymbols() {
        // GIVEN
        String[] symbols = {"AAPL", "GOOGL", "MSFT"};

        // WHEN
        Flux<StockData> watchlist = stockService.watchlist(symbols).take(5);

        // THEN
        StepVerifier.create(watchlist)
                .expectNextMatches(data -> List.of(symbols).contains(data.getSymbol()))
                .expectNextMatches(data -> List.of(symbols).contains(data.getSymbol()))
                .expectNextMatches(data -> List.of(symbols).contains(data.getSymbol()))
                .expectNextMatches(data -> List.of(symbols).contains(data.getSymbol()))
                .expectNextMatches(data -> List.of(symbols).contains(data.getSymbol()))
                .verifyComplete();
    }

    @Test
    void streamStock_ShouldFilterBySymbol() {
        // GIVEN
        String symbol = "AAPL";

        // WHEN
        Flux<StockData> stream = stockService.streamStock(symbol).take(3);

        // THEN
        StepVerifier.create(stream)
                .expectNextMatches(data -> data.getSymbol().equals(symbol))
                .expectNextMatches(data -> data.getSymbol().equals(symbol))
                .expectNextMatches(data -> data.getSymbol().equals(symbol))
                .verifyComplete();
    }

    @Test
    void multipleCurrentPriceRequests_ShouldAllComplete() {
        // GIVEN
        List<String> symbols = List.of("AAPL", "GOOGL", "MSFT", "AMZN", "TSLA");

        // WHEN
        Flux<StockData> results = Flux.fromIterable(symbols)
                .flatMap(stockService::currentPrice)
                .collectList()
                .flatMapMany(Flux::fromIterable);

        // THEN
        StepVerifier.create(results)
                .expectNextCount(5)
                .verifyComplete();
    }
}
