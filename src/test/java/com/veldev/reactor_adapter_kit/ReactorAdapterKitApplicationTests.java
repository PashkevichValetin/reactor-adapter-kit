package com.veldev.reactor_adapter_kit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class ReactorAdapterKitApplicationTests {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void contextLoads() {
        assertThat(applicationContext).isNotNull();
        assertThat(applicationContext.getBeanDefinitionCount()).isGreaterThan(0);
    }

    @Test
    void shouldHaveRequiredBeans() {
        assertThat(applicationContext.containsBean("reactiveStockService")).isTrue();
        assertThat(applicationContext.containsBean("stockController")).isTrue();
        assertThat(applicationContext.containsBean("corsConfig")).isTrue();
    }

    @Test
    void shouldDemonstrateReactiveProgramming() {
        Flux<Integer> numbers = Flux.just(1, 2, 3, 4, 5)
                .delayElements(Duration.ofMillis(10))
                .map(i -> i * 2);

        StepVerifier.create(numbers)
                .expectNext(2, 4, 6, 8, 10)
                .verifyComplete();
    }

    @Test
    void shouldHandleReactiveErrors() {
        Flux<String> fluxWithError = Flux.just("A", "B", "C")
                .concatWith(Flux.error(new RuntimeException("Test error")))
                .map(String::toLowerCase);

        StepVerifier.create(fluxWithError)
                .expectNext("a", "b", "c")
                .expectError(RuntimeException.class)
                .verify();
    }

}
