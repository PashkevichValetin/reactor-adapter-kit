package com.veldev.reactor_adapter_kit.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class WeatherData {

    private String city;
    private Double temperature;
    private String condition;
    private LocalDateTime timestamp;

}
