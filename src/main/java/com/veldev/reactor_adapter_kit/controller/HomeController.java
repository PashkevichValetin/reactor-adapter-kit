package com.veldev.reactor_adapter_kit.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.result.view.RedirectView;
import reactor.core.publisher.Mono;

@RestController
public class HomeController {
    @GetMapping("/")
    public Mono<RedirectView> home() {
        RedirectView redirectView = new RedirectView();
        redirectView.setUrl("/api/stocks/health");
        return Mono.just(redirectView);
    }
}