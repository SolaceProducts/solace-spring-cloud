package com.solace.spring.cloud.stream.binder.instrumentation.springBootTests.app;

import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("dynamicDestinationProcessor")
public class DynamicDestinationProcessorConfig {

  private static final Logger log = LoggerFactory.getLogger(
      DynamicDestinationProcessorConfig.class);

  @Bean
  public Consumer<String> dynamicDestinationProcessor1(StreamBridge streamBridge) {
    return message -> {
      String topic = "solace/dynamicDestination/hello";
      log.info("Received: {}", message);
      String output = message.toUpperCase();
      log.info("Sending: {}", output);
      log.info("Setting dynamic target destination (using StreamBridge) to: {}", topic);
      streamBridge.send(topic, output);
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