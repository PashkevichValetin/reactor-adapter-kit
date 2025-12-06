package com.veldev.reactor_adapter_kit.adapters;

import com.veldev.reactor_adapter_kit.model.WeatherData;
import com.veldev.reactor_adapter_kit.services.WeatherService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;

@Slf4j
@Component
public class WeatherServiceAdapter implements WeatherService {

    private final Random random = new Random();
    private final String[] conditions = {"Sunny", "Cloudy", "Rainy", "Snowy", "Windy", "Foggy"};

    @Override
    public Flux<WeatherData> streamWeather(String city) {
        log.info("Stating weather stream for: {}", city); // Прогноз погоды для city

        return Flux.interval(Duration.ofSeconds(2))
                .map(tick -> generateWeatherData(city))
                .doOnNext(data -> log.debug("{}: {}C, {}", data.getCity(), data.getTemperature(), data.getCondition()))
                .doOnSubscribe(sub -> log.info("Client subscribed to {}", city)) //Клиент подписался на city
                .doOnCancel(() -> log.info("Stream stopped for {}", city)); // Трансляция остановлена на city


    }

    @Override
    public Mono<WeatherData> currentWeather(String city) {
        log.info("Fetching current weather for: {}", city); // Получение текущей погоды для city

        return Mono.fromCallable(() -> generateWeatherData(city))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnSuccess(data -> log.info("Got weather for {}: {}C", city, data.getTimestamp())) // Есть погода для city
                .doOnError(error -> log.error("Error for {}: {}", city, error.getMessage()));
    }

    private WeatherData generateWeatherData(String city) {
        double temperature = round(-10 + random.nextDouble() * 35, 1);
        return WeatherData.builder()
                .city(city)
                .temperature(temperature)
                .condition(getConditionForTemperature(temperature))
                .timestamp(LocalDateTime.now())
                .build();
    }

    private String getConditionForTemperature(double temp) {
        if (temp < 0) {
            return "Snowy";
        }

        if (temp < 10) {
            return "Cloudy";
        }

        if (temp < 20) {
            return conditions[random.nextInt(conditions.length)];
        } else {
            return "Sunny";
        }
    }

    // Рассчитываем и округляем температуру
    private double round(double value, int places) {
        double scale = Math.pow(10, places); // создаем множитель
        return Math.round(value * scale) / scale; // Округляем и делим обратно
    }
}
