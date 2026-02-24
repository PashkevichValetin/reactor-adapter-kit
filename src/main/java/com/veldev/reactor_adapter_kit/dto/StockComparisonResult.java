package com.veldev.reactor_adapter_kit.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record StockComparisonResult(
        StockResponse stock1,
        StockResponse stock2,
        LocalDateTime timestamp,
        Boolean comparisonSuccessful,
        BigDecimal priceDifference,
        BigDecimal priceDifferencePercent,
        String error
) {
}
