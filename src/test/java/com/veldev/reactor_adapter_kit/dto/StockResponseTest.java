package com.veldev.reactor_adapter_kit.dto;

import com.veldev.reactor_adapter_kit.model.StockData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;


public class StockResponseTest {

    @Test
    void shouldCreateStockResponseFromStockData() {
        // GIVEN
        LocalDateTime now = LocalDateTime.now();
        StockData stockData = StockData.builder()
                .symbol("AAPL")
                .price(new BigDecimal("150.25"))
                .change(new BigDecimal("2.50"))
                .changePercent(new BigDecimal("1.69"))
                .volume(1000000L)
                .timestamp(now)
                .build();

        // WHEN
        StockResponse response = StockResponse.from(stockData);

        // THEN
        assertEquals("AAPL", response.symbol());
        assertEquals(new BigDecimal("150.25"), response.price());
        assertEquals(new BigDecimal("2.50"), response.change());
        assertEquals(new BigDecimal("1.69"), response.changePercent());
        assertEquals(1000000L, response.volume());
        assertEquals(now, response.timestamp());
        assertTrue(response.success());
        assertNull(response.error());
    }

    @Test
    void shouldHandleNullErrorMessageInErrorResponse() {
        // GIVEN
        String symbol = "AAPL";

        // WHEN
        StockResponse response = StockResponse.error(symbol, null);

        // THEN
        assertEquals(symbol, response.symbol());
        assertFalse(response.success());
        assertNull(response.error());
    }
}

