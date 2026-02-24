package com.veldev.reactor_adapter_kit.mapper;

import com.veldev.reactor_adapter_kit.dto.StockResponse;
import com.veldev.reactor_adapter_kit.model.StockData;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

@Mapper
public interface StockMapper {
    StockMapper INSTANCE = Mappers.getMapper(StockMapper.class);

    @Mapping(target = "success", constant = "true")
    @Mapping(target = "error", constant = "null")
    StockResponse toStockResponse(StockData data);

    default StockResponse toErrorResponse(String symbol, Throwable error) {
        return StockResponse.error(symbol, error.getMessage());
    }
}