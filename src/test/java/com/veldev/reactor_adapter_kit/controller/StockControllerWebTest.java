package com.veldev.reactor_adapter_kit.controller;

import com.veldev.reactor_adapter_kit.dto.StockComparisonResult;
import com.veldev.reactor_adapter_kit.dto.StockResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Map;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public class StockControllerWebTest {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void getCurrentPrice_ShouldReturnStockResponse() {
        webTestClient.get()
                .uri("/api/stocks/price/AAPL")
                .exchange()
                .expectStatus().isOk()
                .expectBody(StockResponse.class)
                .value(response -> {
                    assert response.symbol().equals("AAPL");
                    assert response.success();
                    assert response.price() != null;
                });
    }

    @Test
    void getCurrentPrice_WithInvalidSymbolReturnErrorResponse() {
        webTestClient.get()
                .uri("/api/stocks/price/INVALID")
                .exchange()
                .expectStatus().isOk()
                .expectBody(StockResponse.class)
                .value(response -> {
                    assert !response.success();
                    assert response.symbol().equals("INVALID");
                });
    }

    @Test
    void getAvailableSymbols_ShouldReturnSymbolsMap() {
        webTestClient.get()
                .uri("/api/stocks/symbols")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .value(response -> {
                    assert response.containsKey("symbols");
                    assert response.containsKey("count");
                    assert (Integer) response.get("count") > 0;
                });
    }

    @Test
    void healthCheck_ShouldReturnUpStatus() {
        webTestClient.get()
                .uri("/api/stocks/health")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .value(response -> {
                    assert response.get("status").equals("UP");
                    assert response.get("service").equals("Stock Service API");
                });
    }

    @Test
    void compareStocks_ShouldReturnComparison() {
        webTestClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/stocks/compare")
                        .queryParam("symbol1", "AAPL")
                        .queryParam("symbol2", "GOOGL")
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBody(StockComparisonResult.class)
                .value(result -> {
                    assert result.comparisonSuccessful();
                    assert result.stock1().symbol().equals("AAPL");
                    assert result.stock2().symbol().equals("GOOGL");
                    assert result.priceDifference() != null;
                });
    }

    @Test
    void streamStock_ShouldReturnServerSentEvents() {
        Flux<String> eventStream = webTestClient.get()
                .uri("/api/stocks/stream/AAPL")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(String.class)
                .getResponseBody();

        StepVerifier.create(eventStream.take(3))
                .expectNextCount(3)
                .thenCancel()
                .verify();
    }

    @Test
    void actuatorHealth_ShouldBeAccessible() {
        webTestClient.get()
                .uri("/actuator/health")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("UP");
    }
}






































