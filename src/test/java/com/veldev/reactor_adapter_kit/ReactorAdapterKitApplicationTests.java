package com.veldev.reactor_adapter_kit;

import com.veldev.reactor_adapter_kit.adapters.WeatherServiceAdapter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest
class ReactorAdapterKitApplicationTests {

    @Autowired
    private WeatherServiceAdapter weatherServiceAdapter;

    @Test
    void contextLoads() {
        assertThat(weatherServiceAdapter).isNotNull();
    }

	@Test
	void testCurrentWeatherReturnsValidData() {
        StepVerifier.create(weatherServiceAdapter.currentWeather("Paris"))
                .assertNext(data -> {
                    assertThat(data.getCity()).isEqualTo("Paris");
                    assertThat(data.getTemperature()).isBetween(-10.0, 25.0);
                    assertThat(data.getCondition()).isNotBlank();
                    assertThat(data.getTimestamp()).isNotNull();
                })
                .verifyComplete();
	}

    @Test
    void testStreamProducesMultipleUpdates() {
        assertThat(weatherServiceAdapter).isNotNull();

        StepVerifier.create(
                weatherServiceAdapter.streamWeather("Berlin")
                        .take(3)
        )
                .expectNextCount(3)
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void testTemperatureMatchesCondition() {
        assertThat(weatherServiceAdapter).isNotNull();

        StepVerifier.create(weatherServiceAdapter.currentWeather("Oslo"))
                .assertNext(data -> {
                    double temp = data.getTemperature();
                    String condition = data.getCondition();

                    if (temp < 0) {
                        assertThat(condition).isEqualTo("Snowy");
                    } else if (temp < 10) {
                        assertThat(condition).isEqualTo("Cloudy");
                    } else if (temp >= 20) {
                        assertThat(condition).isEqualTo("Sunny");
                    }

                })
                .verifyComplete();
    }

}
