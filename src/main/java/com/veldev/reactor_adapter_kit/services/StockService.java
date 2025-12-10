package com.veldev.reactor_adapter_kit.services;

import com.veldev.reactor_adapter_kit.model.StockData;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface StockService {
    Flux<StockData> streamStock(String symbol);
    Mono<StockData> currentPrice(String symbol);
    Flux<String> getAvailableSymbols();
    Flux<StockData> watchlist(String... symbols);
}