package com.veldev.reactor_adapter_kit.services;

import com.veldev.reactor_adapter_kit.model.StockData;

public interface StockCallback {
    void onUpdate(StockData stockData); // Вызывается при успешном обновлении
    void onError(Throwable error); // Вызывается при ошибке
}