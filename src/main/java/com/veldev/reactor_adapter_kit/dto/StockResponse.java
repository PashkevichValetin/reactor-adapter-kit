package com.veldev.reactor_adapter_kit.dto;

import com.veldev.reactor_adapter_kit.model.StockData;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record StockResponse(
        String symbol,
        BigDecimal price,
        BigDecimal change,
        BigDecimal changePercent,
        Long volume,
        LocalDateTime timestamp,
        Boolean success,
        String error
) {
    public static StockResponse from(StockData data) {
        return new StockResponse(
                data.getSymbol(),
                data.getPrice(),
                data.getChange(),
                data.getChangePercent(),
                data.getVolume(),
                data.getTimestamp(),
                true,
                null
        );
    }

    public static StockResponse error(String symbol, String message) {
        return new StockResponse(
                symbol,
                null,
                null,
                null,
                null,
                LocalDateTime.now(),
                false,
                null
        );
    }
}
