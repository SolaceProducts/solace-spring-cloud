package com.solace.spring.cloud.stream.binder.instrumentation;

import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;


@Component
@Profile("consumer")
public class ConsumerConfig {

  private static final Logger log = LoggerFactory.getLogger(ConsumerConfig.class);

  @Bean
  public Consumer<String> consumer1() {
    return message -> log.info("Received: {}", message);
  }

  @Bean
  public Supplier<String> source1() {
    return () -> {
      String message = "Hello World!";
      log.info("Emitting: {}", message);
      return message;
    };
  }
}