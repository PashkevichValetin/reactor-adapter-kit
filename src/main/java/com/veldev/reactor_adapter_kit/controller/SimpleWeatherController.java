package com.veldev.reactor_adapter_kit.controller;

import com.veldev.reactor_adapter_kit.model.WeatherData;
import com.veldev.reactor_adapter_kit.services.WeatherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.awt.*;
import java.time.Duration;

@RestController
@Slf4j
@RequestMapping("/api/weather")
@RequiredArgsConstructor
public class SimpleWeatherController {

    private final WeatherService weatherService;

    @GetMapping(value = "/stream/{city}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<WeatherData> streamWeather(@PathVariable String city) {
        log.info("Starting SSE stream for city: {}", city); // Запуск потока SSE для города

        return weatherService.streamWeather(city)
                .doOnSubscribe(sub -> log.info("Client subscribed to {} stream", city))
                .doOnNext(data -> log.info("Temperature {}: {}C ({})", data.getCity(), data.getTemperature(),
                        data.getCondition()))
                .doOnCancel(() -> log.info("Stream stopped for {}", city)); // Трансляция остановлена на city
    }

    @GetMapping("/{city}")
    public Mono<WeatherData> getWeather(@PathVariable String city) {
        log.info("Current weather requested for {}", city); // Текущая погода запрошена для city

        return weatherService.currentWeather(city)
                .doOnSuccess(data -> log.info("{}: {}C, {}", city, data.getTemperature(), data.getCondition()));
    }

    @GetMapping("/compare")
    public Flux<String> compareCities(
            @RequestParam(defaultValue = "Moscow,London") String cities,
            @RequestParam(defaultValue = "10") int updates) {

        log.info("Comparing cities: {} ({} updates)", cities, updates); // Сравнение городов

        String[] cityArray = cities.split(",");
        Flux<String>[] streams = new Flux[cityArray.length];

        for (int i = 0; i < cityArray.length; i++) {
            String city = cityArray[i].trim();
            streams[i] = weatherService.streamWeather(city)
                    .map(data -> String.format("%s: %+.1fC | %s | %s",
                            data.getCity(),
                            data.getTemperature(),
                            data.getCondition(),
                            data.getTimestamp().toLocalTime()));
        }

        return Flux.merge(streams)
                .take(updates)
                .doOnNext(log::info)
                .doOnComplete(() -> log.info("Comparison completed")); //Сравнение завершено
    }

    @GetMapping("/dashboard")
    public Flux<WeatherData> weatherDashboard() {
        log.info("Weather dashboard started"); // Панель погоды запущена
        return Flux.merge(
                        weatherService.streamWeather("Moscow"),
                        weatherService.streamWeather("London"),
                        weatherService.streamWeather("Minsk"),
                        weatherService.streamWeather("Kyiv")
                ).buffer(Duration.ofSeconds(5))
                .flatMap(Flux::fromIterable)
                .doOnNext(data -> log.info("Dashboard: {} {}C",
                        data.getCity(), data.getTemperature())); // Панель управления
    }

    @GetMapping("/health")
    public Mono<String> health() {
        return Mono.just("Weather service is healthy!") // Метеорологическая служба в порядке
                .doOnNext(log::info);
    }

    @GetMapping("/demo")
    public Flux<String> demo() {
        log.info("Starting demo mode"); // Запускаем демонстрационный режим

        return Flux.merge(
                        weatherService.streamWeather("Paris")
                                .map(data -> String.format("FR Paris:   %+.1fC", data.getTemperature())),
                        weatherService.streamWeather("Berlin")
                                .map(data -> String.format("DE Berlin:   %+.1fC", data.getTemperature())),
                        weatherService.streamWeather("Madrid")
                                .map(data -> String.format("SE Madrid:   %+.1fC", data.getTemperature()))
                ).take(15)
                .doOnNext(log::info)
                .doOnComplete(() -> log.info("Demo completed")); // Демо завершено
    }

    public static org.slf4j.Logger getLog() {
        return log;
    }

    public WeatherService getWeatherService() {
        return weatherService;
    }
}
























