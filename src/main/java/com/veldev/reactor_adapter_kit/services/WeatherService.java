package com.veldev.reactor_adapter_kit.services;

import com.veldev.reactor_adapter_kit.model.WeatherData;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public interface WeatherService {
    Flux<WeatherData> streamWeather(String city);
    Mono<WeatherData> currentWeather(String city);
}
