package com.solace.spring.cloud.stream.binder.instrumentation;

import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("processor")
public class ProcessorConfig {

  private static final Logger log = LoggerFactory.getLogger(ProcessorConfig.class);

  @Bean
  public Function<String, String> processor1() {
    return message -> {
      log.info("Received: {}", message);

      String output = message.toUpperCase();
      log.info("Sending: {}", output);

      return output;
    };
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