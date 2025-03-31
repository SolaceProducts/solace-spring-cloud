package com.solace.spring.cloud.stream.binder.instrumentation.springBootTests.app;

import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
@Profile("manualAck")
public class ManualAckConfig {

  private static final Logger log = LoggerFactory.getLogger(ManualAckConfig.class);

  @Bean("manualAckFunction")
  @Profile("requeue")
  public Consumer<Message<String>> manualAckFunction1() {
    return message -> {
      log.info("Received: {}", message);

      // Disable Auto-Acknowledgement
      AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(
          message);
      ackCallback.noAutoAck();

      log.info("Requeue: {}", message);

      //Message will be redelivered max 2 times as consumer has configured queueMaxMsgRedelivery: 2
      AckUtils.requeue(ackCallback);
    };
  }

  @Bean("manualAckFunction")
  @Profile({"reject"})
  public Consumer<Message<String>> manualAckFunction2() {
    return message -> {
      log.info("Received: {}", message);

      // Disable Auto-Acknowledgement
      AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(
          message);
      ackCallback.noAutoAck();

      log.info("Reject: {}", message);

      //Message will not be redelivered. It will be moved to DMQ if one is configured.
      AckUtils.reject(ackCallback);
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