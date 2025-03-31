package com.solace.spring.cloud.stream.binder.instrumentation.springBootTests.app;

import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.stereotype.Component;


@Component
@Profile({"bindingErrorHandler", "globalErrorHandler"})
public class ErrorHandlerConfig {

  private static final Logger log = LoggerFactory.getLogger(ErrorHandlerConfig.class);

  @Bean
  public Consumer<String> consumer1() {
    return message -> {
      log.info("Received: {}", message);

      // throw exception
      throw new RuntimeException("Exception thrown");
    };
  }

  @Bean
  public Consumer<ErrorMessage> customErrorHandler() {
    return message -> {
      // ErrorMessage received on a binder
      log.info("Received error message on custom error handler");
      log.info("Original Message: " + message.getOriginalMessage());
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