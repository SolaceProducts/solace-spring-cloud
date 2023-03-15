package com.solace.spring.cloud.stream.binder.springBootTests;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@SpringBootApplication
public class SpringCloudStreamApp {

    public static void main(String[] args) {
        SpringApplication.run(SpringCloudStreamApp.class, args);
    }

    @Bean
    public Consumer<Message<?>> consume() {
        return (msg -> System.out.println(msg.getPayload()));
    }

    @Bean
    public Consumer<Message<?>> otherConsume() {
        return (msg -> System.out.println(msg.getPayload()));
    }

}
