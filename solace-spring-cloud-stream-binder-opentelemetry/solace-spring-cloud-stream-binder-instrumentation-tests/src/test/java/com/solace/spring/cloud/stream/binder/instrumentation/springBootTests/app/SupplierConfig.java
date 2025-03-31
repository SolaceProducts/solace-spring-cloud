package com.solace.spring.cloud.stream.binder.instrumentation.springBootTests.app;

import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("supplier")
public class SupplierConfig {

  private static final Logger log = LoggerFactory.getLogger(SupplierConfig.class);

  @Bean
  public Supplier<String> source1() {
    return () -> {
      String message = "Hello World!";
      log.info("Emitting: {}", message);
      return message;
    };
  }
}