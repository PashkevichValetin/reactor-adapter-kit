package com.veldev.reactor_adapter_kit.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockData {

    private String symbol;
    private Double price;
    private Double change;
    private Double changePercent;
    private Long volume;
    private LocalDateTime timestamp;

    public boolean isUp() {
        return change != null && change > 0;
    }

    public boolean isDown() {
        return change != null && change < 0;
    }

    public boolean isUnchanged() {
        return change == null || change == 0.0;
    }
}
